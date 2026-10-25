use super::*;

/// Command identity resolved from one lowercased 8-byte window, so a command
/// is classified once instead of being re-compared at every dispatch gate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CmdId {
   Other,
   Get,
   Set,
   Del,
   Ping,
   Auth,
   Quit,
   Hello,
   Hget,
   Hset,
   Incr,
   Rpop,
   Lpop,
   Sadd,
   Zadd,
   Xadd,
   Lpush,
   Rpush,
   Pfadd,
   Expire,
   Setbit,
   Geoadd,
   Pfcount,
   Geodist,
   Bitcount,
   Publish,
   JsonSet,
   JsonGet,
}

impl CmdId {
   #[inline(always)]
   pub(super) fn is_queue(self) -> bool {
      matches!(
         self,
         CmdId::Lpush | CmdId::Rpush | CmdId::Lpop | CmdId::Rpop
      )
   }

   #[inline(always)]
   pub(super) fn is_write_run_candidate(self) -> bool {
      matches!(
         self,
         CmdId::Set
            | CmdId::Incr
            | CmdId::Hset
            | CmdId::Sadd
            | CmdId::Zadd
            | CmdId::Xadd
            | CmdId::Pfadd
            | CmdId::Geoadd
            | CmdId::JsonSet
      )
   }
}

/// Space-padded little-endian word for a command name of at most 8 bytes.
#[inline(always)]
const fn cmd_word(name: &[u8]) -> u64 {
   let mut w = [b' '; 8];
   let mut i = 0;
   while i < name.len() {
      w[i] = name[i];
      i += 1;
   }
   u64::from_le_bytes(w)
}

const W_GET: u64 = cmd_word(b"get");
const W_SET: u64 = cmd_word(b"set");
const W_DEL: u64 = cmd_word(b"del");
const W_PING: u64 = cmd_word(b"ping");
const W_AUTH: u64 = cmd_word(b"auth");
const W_QUIT: u64 = cmd_word(b"quit");
const W_HELLO: u64 = cmd_word(b"hello");
const W_HGET: u64 = cmd_word(b"hget");
const W_HSET: u64 = cmd_word(b"hset");
const W_INCR: u64 = cmd_word(b"incr");
const W_RPOP: u64 = cmd_word(b"rpop");
const W_LPOP: u64 = cmd_word(b"lpop");
const W_SADD: u64 = cmd_word(b"sadd");
const W_ZADD: u64 = cmd_word(b"zadd");
const W_XADD: u64 = cmd_word(b"xadd");
const W_LPUSH: u64 = cmd_word(b"lpush");
const W_RPUSH: u64 = cmd_word(b"rpush");
const W_PFADD: u64 = cmd_word(b"pfadd");
const W_EXPIRE: u64 = cmd_word(b"expire");
const W_SETBIT: u64 = cmd_word(b"setbit");
const W_GEOADD: u64 = cmd_word(b"geoadd");
const W_PFCOUNT: u64 = cmd_word(b"pfcount");
const W_GEODIST: u64 = cmd_word(b"geodist");
const W_BITCOUNT: u64 = cmd_word(b"bitcount");
const W_PUBLISH: u64 = cmd_word(b"publish");
const W_JSONSET: u64 = cmd_word(b"json.set");
const W_JSONGET: u64 = cmd_word(b"json.get");

#[inline(always)]
pub(super) fn cmd_id(cmd: &[u8]) -> CmdId {
   let n = cmd.len();
   if n == 0 || n > 8 {
      return CmdId::Other;
   }
   let mut w = [b' '; 8];
   w[..n].copy_from_slice(&cmd[..n]);
   // Bit 5 is the ASCII case bit: setting it folds uppercase letters and
   // leaves the digits and '.' in command names untouched.
   let w = u64::from_le_bytes(w) | 0x2020_2020_2020_2020;
   match (n, w) {
      (3, W_GET) => CmdId::Get,
      (3, W_SET) => CmdId::Set,
      (3, W_DEL) => CmdId::Del,
      (4, W_PING) => CmdId::Ping,
      (4, W_AUTH) => CmdId::Auth,
      (4, W_QUIT) => CmdId::Quit,
      (4, W_HGET) => CmdId::Hget,
      (4, W_HSET) => CmdId::Hset,
      (4, W_INCR) => CmdId::Incr,
      (4, W_RPOP) => CmdId::Rpop,
      (4, W_LPOP) => CmdId::Lpop,
      (4, W_SADD) => CmdId::Sadd,
      (4, W_ZADD) => CmdId::Zadd,
      (4, W_XADD) => CmdId::Xadd,
      (5, W_HELLO) => CmdId::Hello,
      (5, W_LPUSH) => CmdId::Lpush,
      (5, W_RPUSH) => CmdId::Rpush,
      (5, W_PFADD) => CmdId::Pfadd,
      (6, W_EXPIRE) => CmdId::Expire,
      (6, W_SETBIT) => CmdId::Setbit,
      (6, W_GEOADD) => CmdId::Geoadd,
      (7, W_PFCOUNT) => CmdId::Pfcount,
      (7, W_GEODIST) => CmdId::Geodist,
      (7, W_PUBLISH) => CmdId::Publish,
      (8, W_BITCOUNT) => CmdId::Bitcount,
      (8, W_JSONSET) => CmdId::JsonSet,
      (8, W_JSONGET) => CmdId::JsonGet,
      _ => CmdId::Other,
   }
}

#[inline(always)]
pub(super) fn dispatch_get_run(conn: &mut Conn<'_>, first_key: (*const u8, usize)) {
   const BATCH: usize = 256;
   let mut keys: [&[u8]; BATCH] = [&[]; BATCH];
   let mut hashes = [0u64; BATCH];
   let mut tables = [std::ptr::null(); BATCH];
   let mut entries = [std::ptr::null(); BATCH];

   let k0_bytes = unsafe { std::slice::from_raw_parts(first_key.0, first_key.1) };
   keys[0] = k0_bytes;
   let mut count = 1;

   const T2: u32 = u32::from_ne_bytes(*b"*2\r\n");
   while count < BATCH {
      let avail = conn.parser.filled - conn.parser.pos;
      if avail < 20 {
         break;
      }
      let buf = unsafe { conn.parser.rbuf.as_ptr().add(conn.parser.pos) };
      let tag = unsafe { std::ptr::read_unaligned(buf as *const u32) };
      if tag != T2 {
         break;
      }
      if let Some(k_ptr) = conn.parser.try_fast_get_key_at(buf, avail) {
         keys[count] = unsafe { std::slice::from_raw_parts(k_ptr.0, k_ptr.1) };
         count += 1;
      } else {
         break;
      }
   }

   if is_null_store() {
      let out = &mut conn.parser.wbuf;
      out.reserve(count * 11);
      const GET_REPLY_11: &[u8] = b"$5\r\nvalue\r\n";
      let old_len = out.len();
      unsafe {
         out.set_len(old_len + count * 11);
         let mut p = out.as_mut_ptr().add(old_len);
         for _ in 0..count {
            std::ptr::copy_nonoverlapping(GET_REPLY_11.as_ptr(), p, 11);
            p = p.add(11);
         }
      }
      return;
   }

   if count == 1 {
      let (hint, entry) = conn.hint_for(first_key);
      let out = &mut conn.parser.wbuf;
      let Some(key) = part_str(out, first_key) else {
         return;
      };
      if !conn.store.get_to_buf_auto(key, hint, entry, out) {
         out.extend_from_slice(b"$-1\r\n");
      }
      return;
   }

   conn
      .store
      .prefetch_slots(&keys[..count], &mut hashes[..count], &mut tables[..count]);
   conn
      .store
      .probe_and_prefetch_entries(&tables[..count], &hashes[..count], &mut entries[..count]);
   let out = &mut conn.parser.wbuf;
   out.reserve(count * 32);
   for i in 0..count {
      let p = entries[i];
      if !p.is_null() {
         let e = unsafe { &*p.cast::<customhash::Entry<crate::storage::value::StoreValue>>() };
         let state = e.state.load(std::sync::atomic::Ordering::Relaxed);
         let expected = ((hashes[i] as u32 as u64) << 32) | customhash::STATE_OCCUPIED;
         if (state & customhash::STATE_MATCH_MASK) == expected && e.key.eq_bytes(keys[i]) {
            let val = unsafe { (*e.value.get()).assume_init_ref() };
            if val.expires_ms == 0 || !val.is_expired() {
               if let crate::storage::value::FyroDB::String(s) = &val.value {
                  s.write_resp(out);
                  continue;
               }
            } else {
               if let Ok(key_str) = std::str::from_utf8(keys[i]) {
                  conn.store.data.remove(key_str);
               }
               out.extend_from_slice(b"$-1\r\n");
               continue;
            }
         }
      }
      let Ok(key_str) = std::str::from_utf8(keys[i]) else {
         crate::utils::resp::write_err(out, "invalid UTF-8 in request");
         continue;
      };
      if !conn
         .store
         .get_to_buf_auto(key_str, Some(hashes[i]), entries[i], out)
      {
         out.extend_from_slice(b"$-1\r\n");
      }
   }
}

#[inline(always)]
pub(super) fn dispatch_set_run(
   conn: &mut Conn<'_>,
   first_key: (*const u8, usize),
   first_val: (*const u8, usize),
) {
   const BATCH: usize = 256;
   let mut keys: [&[u8]; BATCH] = [&[]; BATCH];
   let mut vals: [(*const u8, usize); BATCH] = [(std::ptr::null(), 0); BATCH];
   let mut hashes = [0u64; BATCH];
   let mut tables = [std::ptr::null(); BATCH];
   let mut entries = [std::ptr::null(); BATCH];

   let k0_bytes = unsafe { std::slice::from_raw_parts(first_key.0, first_key.1) };
   keys[0] = k0_bytes;
   vals[0] = first_val;
   let mut count = 1;

   const T3: u32 = u32::from_ne_bytes(*b"*3\r\n");
   while count < BATCH {
      let avail = conn.parser.filled - conn.parser.pos;
      if avail < 24 {
         break;
      }
      let buf = unsafe { conn.parser.rbuf.as_ptr().add(conn.parser.pos) };
      let tag = unsafe { std::ptr::read_unaligned(buf as *const u32) };
      if tag != T3 {
         break;
      }
      if let Some((k_ptr, v_ptr)) = conn.parser.try_fast_set_key_val_at(buf, avail) {
         keys[count] = unsafe { std::slice::from_raw_parts(k_ptr.0, k_ptr.1) };
         vals[count] = v_ptr;
         count += 1;
      } else {
         break;
      }
   }

   if is_null_store() {
      let out = &mut conn.parser.wbuf;
      out.reserve(count * 5);
      const OK_REPLY_5: &[u8] = b"+OK\r\n";
      let old_len = out.len();
      unsafe {
         out.set_len(old_len + count * 5);
         let mut p = out.as_mut_ptr().add(old_len);
         for _ in 0..count {
            std::ptr::copy_nonoverlapping(OK_REPLY_5.as_ptr(), p, 5);
            p = p.add(5);
         }
      }
      return;
   }

   if count == 1 {
      let (hint, entry) = conn.hint_for(first_key);
      let out = &mut conn.parser.wbuf;
      let Some(key) = part_str(out, first_key) else {
         return;
      };
      let Some(value) = part_str(out, first_val) else {
         return;
      };
      conn.store.set_string_auto(key, hint, entry, value, 0);
      if conn.store.has_replication() {
         conn.store.record_current_value(key);
      }
      out.extend_from_slice(b"+OK\r\n");
      return;
   }

   conn
      .store
      .prefetch_slots_write(&keys[..count], &mut hashes[..count], &mut tables[..count]);
   conn.store.probe_and_prefetch_entries_write(
      &tables[..count],
      &hashes[..count],
      &mut entries[..count],
   );
   let out = &mut conn.parser.wbuf;
   out.reserve(count * 5);
   let old_len = out.len();
   let mut success = true;
   for i in 0..count {
      let p = entries[i];
      if !p.is_null() && !conn.store.has_replication() {
         let e = unsafe { &*p.cast::<customhash::Entry<crate::storage::value::StoreValue>>() };
         let expected = ((hashes[i] as u32 as u64) << 32) | customhash::STATE_OCCUPIED;
         let mut current = e.state.load(std::sync::atomic::Ordering::Relaxed);
         if (current & customhash::STATE_MATCH_MASK) == expected && e.key.eq_bytes(keys[i]) {
            let val_bytes = unsafe { std::slice::from_raw_parts(vals[i].0, vals[i].1) };
            let mut updated = false;
            for _ in 0..4 {
               let locked = customhash::bump_seq(current) | customhash::STATE_LOCK;
               match e.state.compare_exchange_weak(
                  current,
                  locked,
                  std::sync::atomic::Ordering::Acquire,
                  std::sync::atomic::Ordering::Relaxed,
               ) {
                  Ok(_) => {
                     conn.store.data.snap_before_write(e);
                     let val = unsafe { (*e.value.get()).assume_init_mut() };
                     val.expires_ms = 0;
                     if let crate::storage::value::FyroDB::String(ref mut s) = val.value {
                        s.overwrite_bytes(val_bytes);
                     } else {
                        val.value = crate::storage::value::FyroDB::String(
                           crate::storage::value::SmallStr::from_slice(val_bytes),
                        );
                     }
                     let unlocked = (customhash::bump_seq(locked) & !customhash::STATE_LOCK)
                        | customhash::STATE_OCCUPIED;
                     e.state
                        .store(unlocked, std::sync::atomic::Ordering::Release);
                     conn.store.data.touched_bytes(keys[i]);
                     updated = true;
                     break;
                  }
                  Err(observed) => {
                     current = observed;
                     if (current & customhash::STATE_MATCH_MASK) != expected {
                        break;
                     }
                     std::hint::spin_loop();
                  }
               }
            }
            if updated {
               continue;
            }
         }
      }

      let Ok(key_str) = std::str::from_utf8(keys[i]) else {
         crate::utils::resp::write_err(out, "invalid UTF-8 in request");
         success = false;
         continue;
      };
      let val_bytes = unsafe { std::slice::from_raw_parts(vals[i].0, vals[i].1) };
      let Ok(val_str) = std::str::from_utf8(val_bytes) else {
         crate::utils::resp::write_err(out, "invalid UTF-8 in request");
         success = false;
         continue;
      };
      conn
         .store
         .set_string_auto(key_str, Some(hashes[i]), entries[i], val_str, 0);
      if conn.store.has_replication() {
         conn.store.record_current_value(key_str);
      }
   }
   if success {
      unsafe {
         out.set_len(old_len + count * 5);
         let mut p = out.as_mut_ptr().add(old_len);
         for _ in 0..count {
            std::ptr::copy_nonoverlapping(b"+OK\r\n".as_ptr(), p, 5);
            p = p.add(5);
         }
      }
   }
}

pub(super) fn dispatch_json_set_run(conn: &mut Conn<'_>, first_raw: &[(*const u8, usize)]) -> bool {
   if first_raw.len() != 4 {
      return false;
   }
   let first_path = unsafe { part_bytes(first_raw[2]) };
   if first_path != b"." {
      return false;
   }
   const BATCH: usize = 128;
   let mut keys: [&[u8]; BATCH] = [&[]; BATCH];
   let mut vals: [(*const u8, usize); BATCH] = [(std::ptr::null(), 0); BATCH];
   let mut hashes = [0u64; BATCH];
   let mut tables = [std::ptr::null(); BATCH];
   let mut entries = [std::ptr::null(); BATCH];

   keys[0] = unsafe { part_bytes(first_raw[1]) };
   vals[0] = first_raw[3];
   let mut count = 1;

   while count < BATCH {
      let avail = conn.parser.filled - conn.parser.pos;
      if avail < 32 {
         break;
      }
      let buf = unsafe { conn.parser.rbuf.as_ptr().add(conn.parser.pos) };
      if let Some((k_ptr, v_ptr)) = conn.parser.try_fast_json_set_key_val_at(buf, avail) {
         keys[count] = unsafe { std::slice::from_raw_parts(k_ptr.0, k_ptr.1) };
         vals[count] = v_ptr;
         count += 1;
      } else {
         break;
      }
   }

   if count == 1 {
      let (hint, entry) = conn.hint_for(first_raw[1]);
      let out = &mut conn.parser.wbuf;
      let Some(key) = part_str(out, first_raw[1]) else {
         return false;
      };
      let Some(val) = part_str(out, first_raw[3]) else {
         return false;
      };
      conn.store.set_json_auto(key, hint, entry, val);
      if conn.store.has_replication() {
         conn.store.record_current_value(key);
      }
      crate::utils::resp::write_ok(out);
      return true;
   }

   conn
      .store
      .prefetch_slots_write(&keys[..count], &mut hashes[..count], &mut tables[..count]);
   conn.store.probe_and_prefetch_entries_write(
      &tables[..count],
      &hashes[..count],
      &mut entries[..count],
   );

   let out = &mut conn.parser.wbuf;
   out.reserve(count * 5);
   let old_len = out.len();
   let mut success = true;

   for i in 0..count {
      let Ok(key_str) = std::str::from_utf8(keys[i]) else {
         crate::utils::resp::write_err(out, "invalid UTF-8 in request");
         success = false;
         continue;
      };
      let val_bytes = unsafe { std::slice::from_raw_parts(vals[i].0, vals[i].1) };
      let Ok(val_str) = std::str::from_utf8(val_bytes) else {
         crate::utils::resp::write_err(out, "invalid UTF-8 in request");
         success = false;
         continue;
      };
      conn
         .store
         .set_json_auto(key_str, Some(hashes[i]), entries[i], val_str);
      if conn.store.has_replication() {
         conn.store.record_current_value(key_str);
      }
   }

   if success {
      const OK_REPLY_5: &[u8] = b"+OK\r\n";
      unsafe {
         out.set_len(old_len + count * 5);
         let mut p = out.as_mut_ptr().add(old_len);
         for _ in 0..count {
            std::ptr::copy_nonoverlapping(OK_REPLY_5.as_ptr(), p, 5);
            p = p.add(5);
         }
      }
   }
   true
}

#[inline(always)]
pub(super) fn dispatch_raw(conn: &mut Conn<'_>, raw: &[(*const u8, usize)]) {
   if raw.is_empty() {
      return;
   }

   let cmd_len = raw[0].1;
   let cmd: &[u8] = unsafe { part_bytes(raw[0]) };
   let id = cmd_id(cmd);

   if is_null_store() {
      let out = &mut conn.parser.wbuf;
      if id == CmdId::Get {
         out.extend_from_slice(b"$5\r\nvalue\r\n");
      } else if id == CmdId::Ping {
         out.extend_from_slice(b"+PONG\r\n");
      } else {
         out.extend_from_slice(b"+OK\r\n");
      }
      return;
   }

   if !conn.authenticated {
      handle_unauth(conn, raw, cmd);
      return;
   }

   if id == CmdId::Auth {
      handle_auth(conn, raw);
      return;
   } else if id == CmdId::Hello {
      handle_hello(conn, raw);
      return;
   }

   if !conn.store.cluster.enabled && !conn.store.at_key_capacity() {
      if cmd_len == 3 {
         if id == CmdId::Get && raw.len() == 2 {
            dispatch_get_run(conn, raw[1]);
            return;
         } else if id == CmdId::Set && raw.len() == 3 {
            dispatch_set_run(conn, raw[1], raw[2]);
            return;
         } else if id == CmdId::Del && raw.len() == 2 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            if conn.store.del(key) {
               out.extend_from_slice(b":1\r\n");
            } else {
               out.extend_from_slice(b":0\r\n");
            }
            return;
         }
      } else if cmd_len == 4 {
         if id == CmdId::Hget && raw.len() == 3 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(field) = part_str(out, raw[2]) else {
               return;
            };
            match conn.store.hget_to_buf(key, field, out) {
               Ok(true) => {}
               Ok(false) => crate::utils::resp::write_nil(out),
               Err(_) => crate::utils::resp::write_wrong_type(out),
            }
            return;
         } else if id == CmdId::Hset && raw.len() == 4 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let pair = [(unsafe { part_bytes(raw[2]) }, unsafe { part_bytes(raw[3]) })];
            crate::commands::hash::hset_raw(key, &pair, conn.store, out);
            if conn.store.has_replication() {
               conn.store.record_current_value(key);
            }
            return;
         } else if id == CmdId::Ping {
            if raw.len() == 1 {
               conn.parser.wbuf.extend_from_slice(crate::utils::resp::PONG);
               return;
            } else if raw.len() == 2 {
               let msg = unsafe { part_bytes(raw[1]) };
               crate::utils::resp::write_bulk_bytes(&mut conn.parser.wbuf, msg);
               return;
            }
         } else if id == CmdId::Incr && raw.len() == 2 {
            let (hint, _) = conn.hint_for(raw[1]);
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            match conn.store.incr_hinted(key, hint) {
               Ok(n) => {
                  if conn.store.has_replication() {
                     conn.store.record_current_value(key);
                  }
                  crate::utils::resp::write_integer(out, n)
               }
               Err(e) => crate::utils::resp::write_err(out, e),
            }
            return;
         } else if id == CmdId::Rpop && raw.len() == 2 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            match conn.store.pop_one_to_buf(key, true, out) {
               Ok(true) => {
                  if conn.store.has_replication() {
                     conn.store.record_current_value(key);
                  }
               }
               Ok(false) => out.extend_from_slice(b"$-1\r\n"),
               Err(_) => crate::utils::resp::write_wrong_type(out),
            }
            return;
         } else if id == CmdId::Lpop && raw.len() == 2 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            match conn.store.pop_one_to_buf(key, false, out) {
               Ok(true) => {
                  if conn.store.has_replication() {
                     conn.store.record_current_value(key);
                  }
               }
               Ok(false) => out.extend_from_slice(b"$-1\r\n"),
               Err(_) => crate::utils::resp::write_wrong_type(out),
            }
            return;
         } else if id == CmdId::Sadd && raw.len() == 3 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(member) = part_str(out, raw[2]) else {
               return;
            };
            match conn.store.sadd(key, &[member]) {
               Ok(n) => {
                  if conn.store.has_replication() {
                     conn.store.record_current_value(key);
                  }
                  crate::utils::resp::write_integer(out, n as i64)
               }
               Err(_) => crate::utils::resp::write_wrong_type(out),
            }
            return;
         } else if id == CmdId::Zadd && raw.len() == 4 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(score_str) = part_str(out, raw[2]) else {
               return;
            };
            let Some(member) = part_str(out, raw[3]) else {
               return;
            };
            if let Ok(score) = score_str.parse::<f64>() {
               match conn.store.zadd(key, &[(score, member)], Default::default()) {
                  Ok(n) => {
                     if conn.store.has_replication() {
                        conn.store.record_current_value(key);
                     }
                     crate::utils::resp::write_integer(out, n as i64)
                  }
                  Err(_) => crate::utils::resp::write_wrong_type(out),
               }
            } else {
               out.extend_from_slice(b"-ERR value is not a valid float\r\n");
            }
            return;
         } else if id == CmdId::Xadd && raw.len() == 5 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(id_str) = part_str(out, raw[2]) else {
               return;
            };
            let Some(f) = part_str(out, raw[3]) else {
               return;
            };
            let Some(v) = part_str(out, raw[4]) else {
               return;
            };
            match conn.store.xadd_id(key, id_str, &[(f, v)], None, false) {
               Ok(Some(id)) => id.write_to_resp(out),
               Ok(None) => crate::utils::resp::write_nil(out),
               Err(e) => crate::utils::resp::write_store_err(out, e),
            }
            if conn.store.has_replication() {
               conn.store.record_current_value(key);
            }
            return;
         }
      } else if cmd_len == 5 {
         if id == CmdId::Lpush && raw.len() == 3 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(value) = part_str(out, raw[2]) else {
               return;
            };
            match conn.store.lpush(key, &[value]) {
               Ok(n) => {
                  if conn.store.has_replication() {
                     conn.store.record_current_value(key);
                  }
                  crate::utils::resp::write_integer(out, n as i64)
               }
               Err(_) => crate::utils::resp::write_wrong_type(out),
            }
            return;
         } else if id == CmdId::Rpush && raw.len() == 3 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(value) = part_str(out, raw[2]) else {
               return;
            };
            match conn.store.rpush(key, &[value]) {
               Ok(n) => {
                  if conn.store.has_replication() {
                     conn.store.record_current_value(key);
                  }
                  crate::utils::resp::write_integer(out, n as i64)
               }
               Err(_) => crate::utils::resp::write_wrong_type(out),
            }
            return;
         } else if id == CmdId::Pfadd && raw.len() == 3 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(elem) = part_str(out, raw[2]) else {
               return;
            };
            match conn.store.pfadd(key, &[elem]) {
               Ok(changed) => {
                  if conn.store.has_replication() {
                     conn.store.record_current_value(key);
                  }
                  crate::utils::resp::write_boolean(out, changed);
               }
               Err(e) => crate::utils::resp::write_store_err(out, e),
            }
            return;
         }
      } else if cmd_len == 6 {
         if id == CmdId::Expire && raw.len() == 3 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(secs) = part_str(out, raw[2]) else {
               return;
            };
            let reply = match secs
               .parse::<u64>()
               .ok()
               .and_then(crate::storage::value::expiry_from_secs)
            {
               Some(exp) => {
                  let ok = conn.store.expire_ms(key, exp);
                  if conn.store.has_replication() {
                     conn.store.record_current_value(key);
                  }
                  if ok {
                     crate::utils::resp::ONE
                  } else {
                     crate::utils::resp::ZERO
                  }
               }
               None => b"-ERR invalid expire time in 'expire' command\r\n" as &[u8],
            };
            out.extend_from_slice(reply);
            return;
         } else if id == CmdId::Setbit && raw.len() == 4 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(offset_str) = part_str(out, raw[2]) else {
               return;
            };
            let Some(value_str) = part_str(out, raw[3]) else {
               return;
            };
            if let (Ok(off), Ok(val)) = (offset_str.parse::<u64>(), value_str.parse::<u8>()) {
               if val <= 1 {
                  match conn.store.setbit(key, off, val == 1) {
                     Ok(prev) => {
                        if conn.store.has_replication() {
                           conn.store.record_current_value(key);
                        }
                        crate::utils::resp::write_integer(out, prev as i64);
                     }
                     Err(e) => crate::utils::resp::write_store_err(out, e),
                  }
               } else {
                  out.extend_from_slice(b"-ERR bit is not an integer or out of range\r\n");
               }
            } else {
               out.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
            }
            return;
         } else if id == CmdId::Geoadd && raw.len() == 5 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(lon_str) = part_str(out, raw[2]) else {
               return;
            };
            let Some(lat_str) = part_str(out, raw[3]) else {
               return;
            };
            let Some(member) = part_str(out, raw[4]) else {
               return;
            };
            if let (Ok(lon), Ok(lat)) = (lon_str.parse::<f64>(), lat_str.parse::<f64>()) {
               if (-180.0..=180.0).contains(&lon) && (-85.05112878..=85.05112878).contains(&lat) {
                  match conn
                     .store
                     .geoadd(key, &[(lon, lat, member)], false, false, false)
                  {
                     Ok(n) => {
                        if conn.store.has_replication() {
                           conn.store.record_current_value(key);
                        }
                        crate::utils::resp::write_integer(out, n as i64);
                     }
                     Err(e) => crate::utils::resp::write_store_err(out, e),
                  }
               } else {
                  out.extend_from_slice(b"-ERR invalid longitude,latitude pair\r\n");
               }
            } else {
               out.extend_from_slice(b"-ERR value is not a valid float\r\n");
            }
            return;
         }
      } else if cmd_len == 7 {
         if id == CmdId::Pfcount && raw.len() == 2 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            match conn.store.pfcount(&[key]) {
               Ok(n) => crate::utils::resp::write_integer(out, n as i64),
               Err(e) => crate::utils::resp::write_store_err(out, e),
            }
            return;
         } else if id == CmdId::Geodist && raw.len() == 4 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(m1) = part_str(out, raw[2]) else {
               return;
            };
            let Some(m2) = part_str(out, raw[3]) else {
               return;
            };
            match conn
               .store
               .geodist(key, m1, m2, crate::storage::geo::GeoUnit::M)
            {
               Ok(Some(d)) => {
                  crate::utils::resp::write_bulk(out, &crate::utils::util::format_float(d))
               }
               Ok(None) => crate::utils::resp::write_nil(out),
               Err(e) => crate::utils::resp::write_store_err(out, e),
            }
            return;
         }
      } else if cmd_len == 8 {
         if id == CmdId::Bitcount && raw.len() == 2 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            match conn.store.bitcount(key, 0, -1, false) {
               Ok(c) => crate::utils::resp::write_integer(out, c as i64),
               Err(_) => crate::utils::resp::write_wrong_type(out),
            }
            return;
         } else if id == CmdId::JsonSet && raw.len() == 4 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(path) = part_str(out, raw[2]) else {
               return;
            };
            let Some(value) = part_str(out, raw[3]) else {
               return;
            };
            match conn.store.json_set(key, path, value, false, false) {
               Ok(true) => {
                  if conn.store.has_replication() {
                     conn.store.record_current_value(key);
                  }
                  crate::utils::resp::write_ok(out);
               }
               Ok(false) => crate::utils::resp::write_nil(out),
               Err(e) => crate::utils::resp::write_store_err(out, e),
            }
            return;
         } else if id == CmdId::JsonGet {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let res = if raw.len() == 2 {
               conn.store.json_get_to_buf(key, &[], out)
            } else if raw.len() == 3 {
               let Some(path) = part_str(out, raw[2]) else {
                  return;
               };
               conn.store.json_get_to_buf(key, &[path], out)
            } else {
               crate::utils::resp::write_wrong_args(out, "json.get");
               return;
            };
            match res {
               Ok(true) => {}
               Ok(false) => crate::utils::resp::write_nil(out),
               Err(e) => crate::utils::resp::write_store_err(out, e),
            }
            return;
         }
      }
   }

   // Only pay for the write gate when cluster is actually enabled and the
   // command is a write.
   let cluster_is_write = conn.store.cluster.enabled && crate::cluster::is_write_command(cmd);

   if conn.store.at_key_capacity() && crate::storage::capacity::is_denyoom_command(cmd) {
      conn
         .parser
         .wbuf
         .extend_from_slice(crate::storage::capacity::OOM_REPLY);
      return;
   }

   let store = conn.store;
   let _cluster_write_guard = if cluster_is_write {
      Some(store.begin_write(conn.worker))
   } else {
      None
   };

   if conn.store.cluster.enabled {
      if conn.store.replica_installing() {
         conn
            .parser
            .wbuf
            .extend_from_slice(b"-CLUSTERDOWN Replica snapshot is installing\r\n");
         return;
      }
      if conn.store.cluster.is_replica && cluster_is_write {
         conn
            .parser
            .wbuf
            .extend_from_slice(b"-READONLY You can't write against a read only replica.\r\n");
         return;
      }

      conn.refresh_topology();
      let asking = std::mem::take(&mut conn.asking);

      // The argument slice is materialized only for multi-key commands;
      // keyless and single-key commands skip it.
      let decision = match crate::cluster::routing_scope(cmd) {
         crate::cluster::RoutingScope::Keyless => crate::cluster::RouteDecision::Local,
         crate::cluster::RoutingScope::FirstKey => match raw.get(1) {
            Some(&part) => {
               let (topology, routing) = conn.cached_topology();
               crate::cluster::route_single_key(
                  &conn.store.cluster,
                  conn.store.cluster_state_ref(),
                  topology,
                  routing,
                  unsafe { part_bytes(part) },
                  asking,
               )
            }
            None => crate::cluster::RouteDecision::Local,
         },
         crate::cluster::RoutingScope::ManyKeys => {
            const STACK_ARGS: usize = 32;
            let mut stack_args = [&[][..]; STACK_ARGS];
            if raw.len().saturating_sub(1) > STACK_ARGS {
               let args: Vec<&[u8]> = raw[1..]
                  .iter()
                  .map(|&part| unsafe { part_bytes(part) })
                  .collect();
               route_cached(conn, cmd, &args, asking)
            } else {
               for (index, part) in raw[1..].iter().enumerate() {
                  stack_args[index] = unsafe { part_bytes(*part) };
               }
               let args = &stack_args[..raw.len() - 1];
               route_cached(conn, cmd, args, asking)
            }
         }
      };
      match decision {
         crate::cluster::RouteDecision::Local => {}
         other => {
            write_route_decision(&mut conn.parser.wbuf, other);
            return;
         }
      }
   }

   if park::is_blocking_cmd(cmd) {
      park::dispatch_blocking(conn, raw);
      return;
   }
   if park::is_blocking_stream_read(cmd, raw) {
      park::dispatch_stream_read(conn, raw);
      return;
   }

   if cmd_len == 3 {
      if id == CmdId::Set && raw.len() >= 3 {
         let (hint, entry) = conn.hint_for(raw[1]);
         let out = &mut conn.parser.wbuf;
         let Some(key) = part_str(out, raw[1]) else {
            return;
         };
         let Some(value) = part_str(out, raw[2]) else {
            return;
         };
         if raw.len() == 3 {
            conn.store.set_string_auto(key, hint, entry, value, 0);
            if conn.store.has_replication() {
               conn.store.record_current_value(key);
            }
            conn.parser.wbuf.extend_from_slice(b"+OK\r\n");
            return;
         }
      } else if id == CmdId::Get && raw.len() == 2 {
         let (hint, entry) = conn.hint_for(raw[1]);
         let out = &mut conn.parser.wbuf;
         let Some(key) = part_str(out, raw[1]) else {
            return;
         };
         if !conn.store.get_to_buf_auto(key, hint, entry, out) {
            out.extend_from_slice(b"$-1\r\n");
         }
         return;
      } else if id == CmdId::Del && raw.len() == 2 {
         let out = &mut conn.parser.wbuf;
         let Some(key) = part_str(out, raw[1]) else {
            return;
         };
         if conn.store.del(key) {
            conn.parser.wbuf.extend_from_slice(b":1\r\n");
         } else {
            conn.parser.wbuf.extend_from_slice(b":0\r\n");
         }
         return;
      }
   } else if cmd_len == 4 {
      if id == CmdId::Hget && raw.len() == 3 {
         let out = &mut conn.parser.wbuf;
         let Some(key) = part_str(out, raw[1]) else {
            return;
         };
         let Some(field) = part_str(out, raw[2]) else {
            return;
         };
         match conn.store.hget_to_buf(key, field, out) {
            Ok(true) => {}
            Ok(false) => crate::utils::resp::write_nil(out),
            Err(_) => crate::utils::resp::write_wrong_type(out),
         }
         return;
      } else if id == CmdId::Hset && raw.len() >= 4 && (raw.len() - 2).is_multiple_of(2) {
         let out = &mut conn.parser.wbuf;
         let Some(key) = part_str(out, raw[1]) else {
            return;
         };
         if raw.len() == 4 {
            let pair = [(unsafe { part_bytes(raw[2]) }, unsafe { part_bytes(raw[3]) })];
            crate::commands::hash::hset_raw(key, &pair, conn.store, &mut conn.parser.wbuf);
         } else {
            let n_pairs = (raw.len() - 2) / 2;
            const STACK_PAIRS: usize = 16;
            if n_pairs <= STACK_PAIRS {
               let mut stack_pairs = [(&[][..], &[][..]); STACK_PAIRS];
               for i in 0..n_pairs {
                  stack_pairs[i] = (unsafe { part_bytes(raw[2 + i * 2]) }, unsafe {
                     part_bytes(raw[3 + i * 2])
                  });
               }
               crate::commands::hash::hset_raw(
                  key,
                  &stack_pairs[..n_pairs],
                  conn.store,
                  &mut conn.parser.wbuf,
               );
            } else {
               let mut pairs: Vec<(&[u8], &[u8])> = Vec::with_capacity(n_pairs);
               for i in 0..n_pairs {
                  pairs.push((unsafe { part_bytes(raw[2 + i * 2]) }, unsafe {
                     part_bytes(raw[3 + i * 2])
                  }));
               }
               crate::commands::hash::hset_raw(key, &pairs, conn.store, &mut conn.parser.wbuf);
            }
         }
         if conn.store.has_replication() {
            conn.store.record_current_value(key);
         }
         return;
      } else if id == CmdId::Ping {
         if raw.len() == 1 {
            conn.parser.wbuf.extend_from_slice(crate::utils::resp::PONG);
            return;
         } else if raw.len() == 2 {
            let msg = unsafe { part_bytes(raw[1]) };
            crate::utils::resp::write_bulk_bytes(&mut conn.parser.wbuf, msg);
            return;
         }
      } else if id == CmdId::Incr && raw.len() == 2 {
         let out = &mut conn.parser.wbuf;
         let Some(key) = part_str(out, raw[1]) else {
            return;
         };
         match conn.store.incr(key) {
            Ok(n) => {
               if conn.store.has_replication() {
                  conn.store.record_current_value(key);
               }
               crate::utils::resp::write_integer(&mut conn.parser.wbuf, n)
            }
            Err(e) => crate::utils::resp::write_err(&mut conn.parser.wbuf, e),
         }
         return;
      } else if id == CmdId::Rpop && raw.len() == 2 {
         let out = &mut conn.parser.wbuf;
         let Some(key) = part_str(out, raw[1]) else {
            return;
         };
         match conn.store.pop_one_to_buf(key, true, &mut conn.parser.wbuf) {
            Ok(true) => {
               if conn.store.has_replication() {
                  conn.store.record_current_value(key);
               }
            }
            Ok(false) => conn.parser.wbuf.extend_from_slice(b"$-1\r\n"),
            Err(_) => crate::utils::resp::write_wrong_type(&mut conn.parser.wbuf),
         }
         return;
      } else if id == CmdId::Lpop && raw.len() == 2 {
         let out = &mut conn.parser.wbuf;
         let Some(key) = part_str(out, raw[1]) else {
            return;
         };
         match conn.store.pop_one_to_buf(key, false, &mut conn.parser.wbuf) {
            Ok(true) => {
               if conn.store.has_replication() {
                  conn.store.record_current_value(key);
               }
            }
            Ok(false) => conn.parser.wbuf.extend_from_slice(b"$-1\r\n"),
            Err(_) => crate::utils::resp::write_wrong_type(&mut conn.parser.wbuf),
         }
         return;
      } else if id == CmdId::Sadd && raw.len() >= 3 {
         let out = &mut conn.parser.wbuf;
         let Some(key) = part_str(out, raw[1]) else {
            return;
         };
         if raw.len() == 3 {
            let Some(member) = part_str(out, raw[2]) else {
               return;
            };
            match conn.store.sadd(key, &[member]) {
               Ok(n) => {
                  if conn.store.has_replication() {
                     conn.store.record_current_value(key);
                  }
                  crate::utils::resp::write_integer(&mut conn.parser.wbuf, n as i64)
               }
               Err(_) => crate::utils::resp::write_wrong_type(&mut conn.parser.wbuf),
            }
            return;
         }
      }
   } else if cmd_len == 6 && id == CmdId::Expire && raw.len() == 3 {
      let out = &mut conn.parser.wbuf;
      let Some(key) = part_str(out, raw[1]) else {
         return;
      };
      let Some(secs) = part_str(out, raw[2]) else {
         return;
      };
      // Bad input and out-of-range overflow produce the same error the
      // generic path reports, so the fast arm stays protocol-identical.
      let reply = match secs
         .parse::<u64>()
         .ok()
         .and_then(crate::storage::value::expiry_from_secs)
      {
         Some(exp) => {
            let ok = conn.store.expire_ms(key, exp);
            if conn.store.has_replication() {
               conn.store.record_current_value(key);
            }
            if ok {
               crate::utils::resp::ONE
            } else {
               crate::utils::resp::ZERO
            }
         }
         None => b"-ERR invalid expire time in 'expire' command\r\n" as &[u8],
      };
      conn.parser.wbuf.extend_from_slice(reply);
      return;
   } else if cmd_len == 5 && id == CmdId::Lpush && raw.len() == 3 {
      let out = &mut conn.parser.wbuf;
      let Some(key) = part_str(out, raw[1]) else {
         return;
      };
      let Some(value) = part_str(out, raw[2]) else {
         return;
      };
      match conn.store.lpush(key, &[value]) {
         Ok(n) => {
            if conn.store.has_replication() {
               conn.store.record_current_value(key);
            }
            crate::utils::resp::write_integer(&mut conn.parser.wbuf, n as i64)
         }
         Err(_) => crate::utils::resp::write_wrong_type(&mut conn.parser.wbuf),
      }
      return;
   }
   if cmd_len == 9
      && (cmd.eq_ignore_ascii_case(b"FT.SEARCH") || cmd.eq_ignore_ascii_case(b"FT.HYBRID"))
      && raw.len() >= 3
   {
      return ft_search_raw(conn, raw);
   }
   if is_offloaded_ft(cmd) {
      return ft_offload_generic(conn, raw);
   }

   const STACK_CAP: usize = 32;
   if raw.len() <= STACK_CAP {
      let mut arr = [""; STACK_CAP];
      for (i, &part) in raw.iter().enumerate() {
         let Some(s) = part_str(&mut conn.parser.wbuf, part) else {
            return;
         };
         arr[i] = s;
      }
      dispatch(conn, &arr[..raw.len()]);
   } else {
      let mut parts: Vec<&str> = Vec::with_capacity(raw.len());
      for &part in raw.iter() {
         let Some(s) = part_str(&mut conn.parser.wbuf, part) else {
            return;
         };
         parts.push(s);
      }
      dispatch(conn, &parts);
   }
}

/// Route one command against this connection's cached topology snapshot.
#[inline(always)]
pub(super) fn route_cached(
   conn: &Conn<'_>,
   cmd: &[u8],
   args: &[&[u8]],
   asking: bool,
) -> crate::cluster::RouteDecision<'static> {
   let (topology, routing) = conn.cached_topology();
   crate::cluster::route_command_with_snapshot(
      &conn.store.cluster,
      conn.store.cluster_state_ref(),
      topology,
      routing,
      cmd,
      args,
      asking,
   )
}

/// Write a non-Local routing decision into the output buffer.
#[cold]
#[inline(never)]
pub(super) fn write_route_decision(out: &mut Vec<u8>, decision: crate::cluster::RouteDecision<'_>) {
   match decision {
      crate::cluster::RouteDecision::Moved { slot, address } => {
         out.extend_from_slice(b"-MOVED ");
         crate::utils::resp::write_usize(out, slot.value() as usize);
         out.push(b' ');
         out.extend_from_slice(address.as_bytes());
         out.extend_from_slice(b"\r\n");
      }
      crate::cluster::RouteDecision::MovedOwned { slot, address } => {
         out.extend_from_slice(b"-MOVED ");
         crate::utils::resp::write_usize(out, slot.value() as usize);
         out.push(b' ');
         out.extend_from_slice(address.as_bytes());
         out.extend_from_slice(b"\r\n");
      }
      crate::cluster::RouteDecision::Ask { slot, address } => {
         out.extend_from_slice(b"-ASK ");
         crate::utils::resp::write_usize(out, slot.value() as usize);
         out.push(b' ');
         out.extend_from_slice(address.as_bytes());
         out.extend_from_slice(b"\r\n");
      }
      crate::cluster::RouteDecision::CrossSlot => {
         out.extend_from_slice(b"-CROSSSLOT Keys in request don't hash to the same slot\r\n");
      }
      crate::cluster::RouteDecision::Unassigned(_) => {
         out.extend_from_slice(b"-CLUSTERDOWN Hash slot not served\r\n");
      }
      crate::cluster::RouteDecision::Local => {}
   }
}

#[inline(never)]
pub(super) fn handle_auth(conn: &mut Conn<'_>, raw: &[(*const u8, usize)]) {
   if raw.len() == 2 || raw.len() == 3 {
      let pass_idx = if raw.len() == 3 { 2 } else { 1 };
      let Some(pass) = part_str(&mut conn.parser.wbuf, raw[pass_idx]) else {
         return;
      };
      if let Some(expected) = conn.auth_required {
         if constant_time_eq(pass.as_bytes(), expected.as_bytes()) {
            conn.authenticated = true;
            conn.parser.wbuf.extend_from_slice(b"+OK\r\n");
         } else {
            conn.authenticated = false;
            conn
               .parser
               .wbuf
               .extend_from_slice(b"-WRONGPASS invalid username-password pair\r\n");
         }
      } else {
         conn.authenticated = true;
         conn.parser.wbuf.extend_from_slice(b"+OK\r\n");
      }
   } else {
      crate::utils::resp::write_wrong_args(&mut conn.parser.wbuf, "auth");
   }
}

#[inline(never)]
pub(super) fn handle_hello(conn: &mut Conn<'_>, raw: &[(*const u8, usize)]) {
   let mut proto = 2u32;
   let mut auth_pass = None;
   if raw.len() >= 2
      && let Some(p) = part_str(&mut conn.parser.wbuf, raw[1])
   {
      proto = p.parse().unwrap_or(2);
   }
   let mut i = 2;
   while i < raw.len() {
      if let Some(kw) = part_str(&mut conn.parser.wbuf, raw[i])
         && kw.eq_ignore_ascii_case("AUTH")
         && i + 2 < raw.len()
      {
         auth_pass = part_str(&mut conn.parser.wbuf, raw[i + 2]);
         i += 3;
         continue;
      }
      i += 1;
   }

   if let Some(expected) = conn.auth_required
      && let Some(pass) = auth_pass
   {
      if !constant_time_eq(pass.as_bytes(), expected.as_bytes()) {
         conn.authenticated = false;
         conn
            .parser
            .wbuf
            .extend_from_slice(b"-WRONGPASS invalid username-password pair\r\n");
         return;
      }
      conn.authenticated = true;
   }

   if proto == 3 {
      crate::utils::resp3::write_hello_resp3(&mut conn.parser.wbuf);
   } else {
      let out = &mut conn.parser.wbuf;
      crate::utils::resp::write_array_header(out, 14);
      crate::utils::resp::write_bulk(out, "server");
      crate::utils::resp::write_bulk(out, "fyrodb");
      crate::utils::resp::write_bulk(out, "version");
      crate::utils::resp::write_bulk(out, "8.2.0");
      crate::utils::resp::write_bulk(out, "proto");
      crate::utils::resp::write_integer(out, 2);
      crate::utils::resp::write_bulk(out, "id");
      crate::utils::resp::write_integer(out, 1);
      crate::utils::resp::write_bulk(out, "mode");
      crate::utils::resp::write_bulk(out, "standalone");
      crate::utils::resp::write_bulk(out, "role");
      crate::utils::resp::write_bulk(out, "master");
      crate::utils::resp::write_bulk(out, "modules");
      crate::utils::resp::write_array_header(out, 0);
   }
}

#[cold]
#[inline(never)]
pub(super) fn handle_unauth(conn: &mut Conn<'_>, raw: &[(*const u8, usize)], cmd: &[u8]) {
   let id = cmd_id(cmd);
   if id == CmdId::Auth {
      handle_auth(conn, raw);
   } else if id == CmdId::Hello {
      handle_hello(conn, raw);
   } else if id == CmdId::Ping {
      conn.parser.wbuf.extend_from_slice(b"+PONG\r\n");
   } else if id == CmdId::Quit {
      conn.parser.wbuf.extend_from_slice(b"+OK\r\n");
   } else {
      conn
         .parser
         .wbuf
         .extend_from_slice(b"-NOAUTH Authentication required.\r\n");
   }
}

#[inline(never)]
pub(super) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
   if a.len() != b.len() {
      return false;
   }
   let mut diff = 0u8;
   for (x, y) in a.iter().zip(b.iter()) {
      diff |= x ^ y;
   }
   diff == 0
}

/// Offloads search commands to the vector pool to avoid stalling workers;
/// `FYRODB_SEARCH_INLINE=1` overrides to keep them inline.
fn search_offloaded() -> bool {
   static INLINE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
   !*INLINE.get_or_init(|| std::env::var("FYRODB_SEARCH_INLINE").is_ok_and(|v| v == "1"))
}

pub(super) fn is_offloaded_ft(cmd: &[u8]) -> bool {
   cmd.len() > 3
      && cmd[..3].eq_ignore_ascii_case(b"FT.")
      && (cmd.eq_ignore_ascii_case(b"FT.AGGREGATE")
         || cmd.eq_ignore_ascii_case(b"FT.CURSOR")
         || cmd.eq_ignore_ascii_case(b"FT.PROFILE"))
}

/// FT.AGGREGATE / FT.CURSOR / FT.PROFILE: owned copies of the arguments go
/// to the pool with the store handle.
pub(super) fn ft_offload_generic(conn: &mut Conn<'_>, raw: &[(*const u8, usize)]) {
   let mut parts: Vec<String> = Vec::with_capacity(raw.len());
   for &p in raw {
      match std::str::from_utf8(unsafe { part_bytes(p) }) {
         Ok(s) => parts.push(s.to_string()),
         Err(_) => {
            conn
               .parser
               .wbuf
               .extend_from_slice(b"-ERR invalid UTF-8 in request\r\n");
            return;
         }
      }
   }
   let store = Arc::clone(conn.store);
   let run = move |out: &mut Vec<u8>| {
      let refs: Vec<&str> = parts.iter().map(String::as_str).collect();
      crate::commands::ft::execute_ft(&refs, &store, out);
   };
   if search_offloaded() {
      conn.submit(run);
   } else {
      run(&mut conn.parser.wbuf);
   }
}

pub(super) fn ft_search_raw(conn: &mut Conn<'_>, raw: &[(*const u8, usize)]) {
   let inline = !search_offloaded()
      || (raw.len() >= 2 && {
         let idx = unsafe { std::str::from_utf8_unchecked(part_bytes(raw[1])) };
         conn.store.search_state().hnsw_graph_is_clean(idx)
      });

   if inline {
      const STACK_PARTS: usize = 32;
      if raw.len() <= STACK_PARTS {
         let mut stack_parts = [""; STACK_PARTS];
         let mut binaries: Vec<(usize, Vec<u8>)> = Vec::new();
         for (i, &part) in raw.iter().enumerate() {
            let bytes = unsafe { part_bytes(part) };
            match std::str::from_utf8(bytes) {
               Ok(s) => stack_parts[i] = s,
               Err(_) if i >= 3 => {
                  binaries.push((i, bytes.to_vec()));
                  stack_parts[i] = "";
               }
               Err(_) => {
                  conn
                     .parser
                     .wbuf
                     .extend_from_slice(b"-ERR invalid UTF-8 in request\r\n");
                  return;
               }
            }
         }
         crate::commands::ft::search_dispatch(
            &stack_parts[..raw.len()],
            &binaries,
            conn.store,
            &mut conn.parser.wbuf,
         );
         return;
      }
   }

   // Owned copies: the job outlives this read buffer when submitted to pool.
   let mut parts: Vec<String> = Vec::with_capacity(raw.len());
   let mut binaries: Vec<(usize, Vec<u8>)> = Vec::new();
   for (i, &part) in raw.iter().enumerate() {
      let bytes = unsafe { part_bytes(part) };
      match std::str::from_utf8(bytes) {
         Ok(s) => parts.push(s.to_string()),
         Err(_) if i >= 3 => {
            binaries.push((i, bytes.to_vec()));
            parts.push(String::new());
         }
         Err(_) => {
            conn
               .parser
               .wbuf
               .extend_from_slice(b"-ERR invalid UTF-8 in request\r\n");
            return;
         }
      }
   }
   let store = Arc::clone(conn.store);
   let run = move |out: &mut Vec<u8>| {
      let refs: Vec<&str> = parts.iter().map(String::as_str).collect();
      crate::commands::ft::search_dispatch(&refs, &binaries, &store, out);
   };
   if search_offloaded() && !inline {
      conn.submit(run);
   } else {
      run(&mut conn.parser.wbuf);
   }
}

#[cfg(test)]
mod cmd_id_tests {
   use super::{CmdId, cmd_id};

   #[test]
   fn classifies_case_insensitively_and_rejects_others() {
      assert_eq!(cmd_id(b"GET"), CmdId::Get);
      assert_eq!(cmd_id(b"get"), CmdId::Get);
      assert_eq!(cmd_id(b"GeT"), CmdId::Get);
      assert_eq!(cmd_id(b"JSON.SET"), CmdId::JsonSet);
      assert_eq!(cmd_id(b"json.set"), CmdId::JsonSet);
      assert_eq!(cmd_id(b"LPUSH"), CmdId::Lpush);
      assert_eq!(cmd_id(b"BITCOUNT"), CmdId::Bitcount);
      assert_eq!(cmd_id(b""), CmdId::Other);
      assert_eq!(cmd_id(b"GETX"), CmdId::Other);
      assert_eq!(cmd_id(b"FT.SEARCH"), CmdId::Other);
   }
}
