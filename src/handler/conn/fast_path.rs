#![allow(clippy::collapsible_if)]

use super::*;

// Small mixed runs keep entry guards so they do not delay concurrent writes.
const READ_EPOCH_MIN: usize = 32;

const OK_BLOCK: [u8; 256 * 5] = {
   let mut b = [0u8; 256 * 5];
   let mut i = 0;
   while i < 256 * 5 {
      b[i] = b'+';
      b[i + 1] = b'O';
      b[i + 2] = b'K';
      b[i + 3] = b'\r';
      b[i + 4] = b'\n';
      i += 5;
   }
   b
};

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
   if n == 0 || n > 8 || cmd[n - 1] <= b' ' {
      return CmdId::Other;
   }
   let mut w = [b' '; 8];
   unsafe {
      std::ptr::copy_nonoverlapping(cmd.as_ptr(), w.as_mut_ptr(), n);
   }
   // Bit 5 is the ASCII case bit: setting it folds uppercase letters and
   // leaves the digits and '.' in command names untouched.
   let w = u64::from_le_bytes(w) | 0x2020_2020_2020_2020;
   match w {
      W_GET => CmdId::Get,
      W_SET => CmdId::Set,
      W_DEL => CmdId::Del,
      W_PING => CmdId::Ping,
      W_AUTH => CmdId::Auth,
      W_QUIT => CmdId::Quit,
      W_HGET => CmdId::Hget,
      W_HSET => CmdId::Hset,
      W_INCR => CmdId::Incr,
      W_RPOP => CmdId::Rpop,
      W_LPOP => CmdId::Lpop,
      W_SADD => CmdId::Sadd,
      W_ZADD => CmdId::Zadd,
      W_XADD => CmdId::Xadd,
      W_HELLO => CmdId::Hello,
      W_LPUSH => CmdId::Lpush,
      W_RPUSH => CmdId::Rpush,
      W_PFADD => CmdId::Pfadd,
      W_EXPIRE => CmdId::Expire,
      W_SETBIT => CmdId::Setbit,
      W_GEOADD => CmdId::Geoadd,
      W_PFCOUNT => CmdId::Pfcount,
      W_GEODIST => CmdId::Geodist,
      W_PUBLISH => CmdId::Publish,
      W_BITCOUNT => CmdId::Bitcount,
      W_JSONSET if cmd[4] == b'.' => CmdId::JsonSet,
      W_JSONGET if cmd[4] == b'.' => CmdId::JsonGet,
      _ => CmdId::Other,
   }
}

struct PointCache {
   key: [u8; 32],
   key_len: usize,
   reply: [u8; 32],
   reply_len: usize,
   stamp: Option<customhash::ReadStamp>,
   /// Consecutive stamp checks that failed because some writer ran.
   failures: u32,
   /// Repeated-key GETs left to serve without checking or learning a stamp.
   backoff: u32,
}

impl PointCache {
   /// Starts tracking a new key (at most 32 bytes). Backoff and failure counts
   /// belong to the previous key: carried over, a key that was contended
   /// would leave the next hot key uncached for up to `POINT_CACHE_BACKOFF`
   /// GETs.
   fn switch_to(&mut self, key: &[u8]) {
      self.key[..key.len()].copy_from_slice(key);
      self.key_len = key.len();
      self.stamp = None;
      self.failures = 0;
      self.backoff = 0;
   }
}

/// After this many consecutive invalidated stamps the cache stops checking:
/// under concurrent writes each check reads every writer stripe's line from
/// another core and still misses.
const POINT_CACHE_MAX_FAILURES: u32 = 4;
const POINT_CACHE_BACKOFF: u32 = 1 << 16;

thread_local! {
   static POINT_CACHE: std::cell::RefCell<PointCache> = const { std::cell::RefCell::new(PointCache {
      key: [0; 32], key_len: 0, reply: [0; 32], reply_len: 0, stamp: None, failures: 0, backoff: 0,
   }) };
}

#[inline(always)]
fn dispatch_get_one<const STREAM_IO: bool>(
   conn: &mut Conn<'_>,
   key_part: (*const u8, usize),
   pin: &customhash::Guard,
) {
   let store = conn.store;
   let key = unsafe { part_bytes(key_part) };
   let mut learn = None;
   if key.len() <= 32 {
      let hit = POINT_CACHE.with_borrow_mut(|cache| {
         if cache.key_len == key.len() && &cache.key[..key.len()] == key {
            if cache.backoff != 0 {
               cache.backoff -= 1;
               return false;
            }
            if let Some(stamp) = cache.stamp.as_ref() {
               if store.data.stamp_is_current(stamp) {
                  cache.failures = 0;
                  conn
                     .parser
                     .wbuf
                     .extend_from_slice(&cache.reply[..cache.reply_len]);
                  return true;
               }
               cache.failures += 1;
               if cache.failures >= POINT_CACHE_MAX_FAILURES {
                  cache.failures = 0;
                  cache.stamp = None;
                  cache.backoff = POINT_CACHE_BACKOFF;
                  return false;
               }
            }
            cache.stamp = None;
            learn = store.data.read_stamp();
         } else {
            cache.switch_to(key);
         }
         false
      });
      if hit {
         return;
      }
   }
   let Some(read) = store.data.read_pinned_bytes(key, pin) else {
      return crate::utils::resp::write_nil(&mut conn.parser.wbuf);
   };
   if read.is_expired() {
      drop(read);
      if !store.data.read_epoch_active() {
         store.data.remove_if_bytes(key, |v| v.is_expired());
      }
      return crate::utils::resp::write_nil(&mut conn.parser.wbuf);
   }
   let Some(value) = read.value.as_string() else {
      return crate::utils::resp::write_nil(&mut conn.parser.wbuf);
   };
   if STREAM_IO
      && value.len() >= super::bulk::DIRECT_SEND_MIN
      && conn.write_offset == conn.parser.wbuf.len()
   {
      send_large_value(conn, value.as_bytes());
      return;
   }
   let start = conn.parser.wbuf.len();
   value.write_resp(&mut conn.parser.wbuf);
   if let Some(stamp) = learn
      && value.is_inline()
      && read.expires_ms == 0
   {
      POINT_CACHE.with_borrow_mut(|cache| {
         let reply = &conn.parser.wbuf[start..];
         cache.reply[..reply.len()].copy_from_slice(reply);
         cache.reply_len = reply.len();
         cache.stamp = Some(stamp);
      });
   }
}

fn send_large_value(conn: &mut Conn<'_>, payload: &[u8]) {
   use std::io::IoSlice;

   let mut hdr = [0u8; 24];
   let hl = crate::utils::resp::format_bulk_header(payload.len(), &mut hdr);
   let total = hl + payload.len() + 2;

   let mut written = 0;
   let mut slices = [
      IoSlice::new(&hdr[..hl]),
      IoSlice::new(payload),
      IoSlice::new(b"\r\n"),
   ];
   let mut pending = &mut slices[..];
   for _ in 0..32 {
      if pending.is_empty() {
         break;
      }
      match conn.stream.write_vectored(pending) {
         Ok(0) => break,
         Ok(n) => {
            written += n;
            if written >= total {
               conn.parser.wbuf.clear();
               conn.write_offset = 0;
               return;
            }
            IoSlice::advance_slices(&mut pending, n);
         }
         Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
         Err(_) => break,
      }
   }

   conn.parser.wbuf.clear();
   conn.parser.wbuf.reserve(total - written);
   let mut pos = written;
   for src in [&hdr[..hl], payload, b"\r\n" as &[u8]] {
      if pos >= src.len() {
         pos -= src.len();
         continue;
      }
      conn.parser.wbuf.extend_from_slice(&src[pos..]);
      pos = 0;
   }
   conn.write_offset = 0;
}

#[inline(always)]
fn same_parts(a: &[u8], b: &[u8]) -> bool {
   a.len() == b.len() && (std::ptr::eq(a.as_ptr(), b.as_ptr()) || a == b)
}

#[inline(always)]
pub(super) fn dispatch_get_run<const STREAM_IO: bool>(
   conn: &mut Conn<'_>,
   first_key: (*const u8, usize),
   read_only: bool,
   pin: &customhash::Guard,
) {
   if conn.parser.filled - conn.parser.pos < 20 && !is_null_store() {
      dispatch_get_one::<true>(conn, first_key, pin);
      return;
   }
   dispatch_get_batch::<STREAM_IO>(conn, first_key, read_only, pin);
}

#[inline(never)]
fn dispatch_repeated_get<const STREAM_IO: bool>(
   conn: &mut Conn<'_>,
   key: &[u8],
   count: usize,
   pin: &customhash::Guard,
) {
   let store = conn.store;
   let out = &mut conn.parser.wbuf;
   let start = out.len();
   let read = store.data.read_pinned_bytes(key, pin);
   if let Some(value) = read
      .as_deref()
      .filter(|v| !v.is_expired())
      .and_then(|v| v.value.as_string())
   {
      if STREAM_IO && value.len() >= super::bulk::MIN_STREAM_BYTES && store.worker_capacity() > 1 {
         super::bulk::send_repeated_value(
            &mut conn.stream,
            out,
            &mut conn.write_offset,
            value.as_bytes(),
            count,
         );
         return;
      }
      value.write_resp(out);
      drop(read);
   } else {
      let expired = read.as_deref().is_some_and(|v| v.is_expired());
      drop(read);
      if expired && !store.data.read_epoch_active() {
         store.data.remove_if_bytes(key, |v| v.is_expired());
      }
      crate::utils::resp::write_nil(out);
   }
   crate::utils::resp::repeat_reply(out, start, count);
}

#[inline(never)]
fn dispatch_get_batch<const STREAM_IO: bool>(
   conn: &mut Conn<'_>,
   first_key: (*const u8, usize),
   read_only: bool,
   pin: &customhash::Guard,
) {
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
      dispatch_get_one::<true>(conn, first_key, pin);
      return;
   }

   if same_parts(keys[0], keys[1]) && keys[2..count].iter().all(|&key| same_parts(key, keys[0])) {
      dispatch_repeated_get::<STREAM_IO>(conn, keys[0], count, pin);
      return;
   }
   let store = conn.store;
   let read_epoch = if read_only && count >= 4 {
      store.data.try_read_epoch()
   } else {
      None
   };

   conn
      .store
      .prefetch_slots(&keys[..count], &mut hashes[..count], &mut tables[..count]);
   // Retry read epoch once before falling back to per-entry guards.
   let read_epoch = if read_epoch.is_none() && read_only && count >= 4 {
      store.data.try_read_epoch()
   } else {
      read_epoch
   };
   conn
      .store
      .probe_and_prefetch_entries(&tables[..count], &hashes[..count], &mut entries[..count]);
   let first_payload_len = if let Some(epoch) = read_epoch.as_ref() {
      unsafe { store.string_payload_len_epoch(hashes[0], entries[0], epoch) }
   } else {
      unsafe { store.string_payload_len_unguarded(hashes[0], entries[0]) }
   };
   let mut read_epoch = if read_epoch.is_none()
      && read_only
      && (count >= READ_EPOCH_MIN || first_payload_len >= super::bulk::MIN_STREAM_BYTES)
   {
      store.data.try_read_epoch()
   } else {
      read_epoch
   };
   let prefetch_payloads = (24..=8192).contains(&first_payload_len)
      && (first_payload_len <= 1024 || store.worker_capacity() == 1);
   let stream_minimum = if store.worker_capacity() == 1 {
      4 * 1024
   } else {
      super::bulk::MIN_STREAM_BYTES
   };
   let stream_floor = if read_epoch.is_some() {
      stream_minimum
   } else {
      super::bulk::DIRECT_SEND_MIN
   };
   if STREAM_IO
      && first_payload_len >= stream_floor
      && unsafe {
         super::bulk::try_stream_get_run(
            conn,
            &keys[..count],
            &hashes[..count],
            &entries[..count],
            &mut read_epoch,
         )
      }
   {
      return;
   }
   let mut i = 0;
   while i < count {
      if conn.parser.wbuf.len() >= GET_BATCH_FLUSH_BYTES {
         if !conn.do_write() {
            return;
         }
      }
      if prefetch_payloads
         && i + 16 < count
         && let Some(epoch) = &read_epoch
      {
         unsafe { store.prefetch_string_epoch(entries[i + 16], epoch) };
      }
      let mut end = i + 1;
      while end < count && same_parts(keys[end], keys[i]) {
         end += 1;
      }
      let run_len = end - i;
      let start_offset = conn.parser.wbuf.len();
      let found = unsafe {
         if let Some(epoch) = &read_epoch {
            store.get_to_buf_epoch_entry_bytes(
               keys[i],
               hashes[i],
               entries[i].cast(),
               epoch,
               &mut conn.parser.wbuf,
            )
         } else {
            store.get_to_buf_fast_entry_bytes(
               keys[i],
               hashes[i],
               entries[i].cast(),
               &mut conn.parser.wbuf,
            )
         }
      };
      if !found {
         conn.parser.wbuf.extend_from_slice(b"$-1\r\n");
      }
      if run_len > 1 {
         crate::utils::resp::repeat_reply(&mut conn.parser.wbuf, start_offset, run_len);
      }
      i = end;
   }
}

/// Flush a GET batch's replies once this much is buffered, which bounds
/// per-connection output memory (8 MiB per connection before).
const GET_BATCH_FLUSH_BYTES: usize = 1024 * 1024;

#[inline(always)]
fn dispatch_set_one(
   conn: &mut Conn<'_>,
   key_part: (*const u8, usize),
   value_part: (*const u8, usize),
   pin: &customhash::Guard,
) {
   let (hint, entry) = conn.hint_for(key_part);
   let out = &mut conn.parser.wbuf;
   let key = unsafe { part_bytes(key_part) };
   let bytes = unsafe { part_bytes(value_part) };
   conn
      .store
      .set_string_auto_bytes_pinned(key, hint, entry, bytes, 0, pin);
   if conn.store.has_replication() {
      conn.store.record_current_value_bytes(key);
   }
   out.extend_from_slice(b"+OK\r\n");
}

#[inline(always)]
pub(super) fn dispatch_set_run(
   conn: &mut Conn<'_>,
   first_key: (*const u8, usize),
   first_val: (*const u8, usize),
   pin: &customhash::Guard,
) {
   if conn.parser.filled - conn.parser.pos < 24 && !is_null_store() {
      dispatch_set_one(conn, first_key, first_val, pin);
      return;
   }
   if first_val.1 >= 8192 {
      dispatch_set_batch::<64, true>(conn, first_key, first_val, pin);
   } else {
      dispatch_set_batch::<256, false>(conn, first_key, first_val, pin);
   }
}

#[inline(never)]
fn dispatch_set_batch<const BATCH: usize, const STREAMING: bool>(
   conn: &mut Conn<'_>,
   first_key: (*const u8, usize),
   first_val: (*const u8, usize),
   pin: &customhash::Guard,
) {
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
      let old_len = out.len();
      unsafe {
         out.set_len(old_len + count * 5);
         std::ptr::copy_nonoverlapping(OK_BLOCK.as_ptr(), out.as_mut_ptr().add(old_len), count * 5);
      }
      return;
   }

   if count == 1 {
      dispatch_set_one(conn, first_key, first_val, pin);
      return;
   }

   let store = conn.store;
   if count >= 4
      && !store.has_replication()
      && !store.data.observing()
      && same_parts(keys[0], keys[count - 1])
      && keys[1..count - 1]
         .iter()
         .all(|key| same_parts(keys[0], key))
   {
      let (hash, shard) = store.data.locate_bytes(keys[0]);
      if let Some(entry) = store.data.probe_entry_fast_bytes(keys[0], hash, shard) {
         let value = unsafe { part_bytes(vals[count - 1]) };
         // The existing-key batch already coalesces consecutive overwrites under one entry lock.
         unsafe { store.set_string_fast_entry_bytes(keys[0], hash, entry, value, 0) };
         conn.parser.wbuf.extend_from_slice(&OK_BLOCK[..count * 5]);
         return;
      }
   }
   store.prefetch_slots_write(&keys[..count], &mut hashes[..count], &mut tables[..count]);
   let prefetched = count.min(BATCH);
   store.probe_and_prefetch_entries_write(
      &tables[..prefetched],
      &hashes[..prefetched],
      &mut entries[..prefetched],
   );
   let write_epoch = store.data.write_epoch();
   let out = &mut conn.parser.wbuf;
   out.reserve(count * 5);
   let old_len = out.len();
   let mut i = 0;
   while i < count {
      let mut end = i + 1;
      while end < count && keys[end] == keys[i] {
         end += 1;
      }

      let p = entries[i];
      let mut updated = false;
      if !p.is_null() && !conn.store.has_replication() {
         let e = unsafe { &*p.cast::<customhash::Entry<crate::storage::value::StoreValue>>() };
         let mut current = e.state.load(std::sync::atomic::Ordering::Relaxed);
         let expected = ((hashes[i] as u32 as u64) << 32) | customhash::STATE_OCCUPIED;
         if (current & (0xFFFF_FFFF_0000_0000 | customhash::STATE_OCCUPIED)) == expected
            && e.key.eq_bytes(keys[i])
         {
            let last_val = vals[end - 1];
            let val_bytes = unsafe { std::slice::from_raw_parts(last_val.0, last_val.1) };
            let mut backoff = 1u32;
            for _ in 0..512 {
               while current & customhash::STATE_LOCK != 0 {
                  for _ in 0..backoff {
                     std::hint::spin_loop();
                  }
                  if backoff < 128 {
                     backoff <<= 1;
                  }
                  current = e.state.load(std::sync::atomic::Ordering::Relaxed);
               }
               if (current & customhash::STATE_OCCUPIED) == 0 || !e.key.eq_bytes(keys[i]) {
                  break;
               }
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
                     let old_expires = val.expires_ms;
                     val.expires_ms = 0;
                     if let crate::storage::value::FyroDB::String(ref mut s) = val.value {
                        s.overwrite_bytes(val_bytes);
                     } else {
                        val.value = crate::storage::value::FyroDB::String(
                           crate::storage::value::SmallStr::from_slice(val_bytes),
                        );
                     }
                     customhash::unlock_write(&e.state, locked, true);
                     conn.store.data.touched_bytes(keys[i]);
                     if old_expires != 0 {
                        conn.store.sub_ttl();
                     }
                     updated = true;
                     break;
                  }
                  Err(observed) => {
                     current = observed;
                     for _ in 0..backoff {
                        std::hint::spin_loop();
                     }
                     if backoff < 128 {
                        backoff <<= 1;
                     }
                  }
               }
            }
         }
      }

      if !updated {
         for k in i..end {
            let key_bytes = keys[k];
            let val_bytes = unsafe { std::slice::from_raw_parts(vals[k].0, vals[k].1) };
            if entries[k].is_null() && !conn.store.has_replication() {
               let written = unsafe {
                  store.data.insert_slot_fast_located_pinned_bytes(
                     hashes[k],
                     store.data.shard_of(hashes[k]),
                     key_bytes,
                     crate::storage::value::StoreValue::string_small(
                        crate::storage::value::SmallStr::from_slice(val_bytes),
                     ),
                     &write_epoch,
                     pin,
                  )
               };
               if written {
                  store.data.touched_bytes(key_bytes);
               } else {
                  store.set_string_fast_bytes(key_bytes, hashes[k], val_bytes);
               }
            } else {
               conn.store.set_string_auto_bytes(
                  key_bytes,
                  Some(hashes[k]),
                  entries[k],
                  val_bytes,
                  0,
               );
               if conn.store.has_replication() {
                  conn.store.record_current_value_bytes(key_bytes);
               }
            }
         }
      }
      i = end;
   }
   unsafe {
      out.set_len(old_len + count * 5);
      std::ptr::copy_nonoverlapping(OK_BLOCK.as_ptr(), out.as_mut_ptr().add(old_len), count * 5);
   }
}

pub(super) fn dispatch_json_set_run(
   conn: &mut Conn<'_>,
   first_raw: &[(*const u8, usize)],
   pin: &customhash::Guard,
) -> bool {
   if first_raw.len() != 4 {
      return false;
   }
   let first_path = unsafe { part_bytes(first_raw[2]) };
   if first_path != b"." {
      return false;
   }
   const BATCH: usize = 256;
   let mut keys: [&[u8]; BATCH] = [&[]; BATCH];
   let mut vals: [(*const u8, usize); BATCH] = [(std::ptr::null(), 0); BATCH];
   let mut hashes = [0u64; BATCH];
   let mut tables = [std::ptr::null(); BATCH];

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
         return true;
      };
      let Some(val) = part_str(out, first_raw[3]) else {
         return true;
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
   let store = conn.store;
   let epoch = store.data.write_epoch();
   let out = &mut conn.parser.wbuf;
   out.reserve(count * 5);

   for i in 0..count {
      let Ok(key_str) = std::str::from_utf8(keys[i]) else {
         crate::utils::resp::write_err(out, "invalid UTF-8 in request");
         continue;
      };
      let val_bytes = unsafe { std::slice::from_raw_parts(vals[i].0, vals[i].1) };
      let Ok(val_str) = std::str::from_utf8(val_bytes) else {
         crate::utils::resp::write_err(out, "invalid UTF-8 in request");
         continue;
      };
      let hash = hashes[i];
      store.data.insert_str_hashed_pinned(
         key_str,
         crate::storage::value::StoreValue::json_str(val_str),
         hash,
         store.data.shard_of(hash),
         &epoch,
         pin,
      );
      crate::utils::resp::write_ok(out);
      if conn.store.has_replication() {
         conn.store.record_current_value(key_str);
      }
   }

   true
}

pub(super) fn dispatch_repeated_read_run(conn: &mut Conn<'_>, first: &[(*const u8, usize)]) {
   let mut saved = [(std::ptr::null(), 0); 8];
   saved[..first.len()].copy_from_slice(first);
   let first = &saved[..first.len()];
   let id = cmd_id(unsafe { part_bytes(first[0]) });
   let mut count = 1;
   while count < 256 {
      let start = conn.parser.pos;
      match conn.parser.parse_one() {
         ParseResult::Complete => {
            let next = &conn.parser.parts_raw;
            if next.len() == first.len()
               && cmd_id(unsafe { part_bytes(next[0]) }) == id
               && next[1..]
                  .iter()
                  .zip(&first[1..])
                  .all(|(&a, &b)| unsafe { part_bytes(a) == part_bytes(b) })
            {
               count += 1;
            } else {
               conn.parser.pos = start;
               break;
            }
         }
         ParseResult::Incomplete => break,
         ParseResult::Error => {
            conn.parser.pos = start;
            break;
         }
      }
   }
   let start = conn.parser.wbuf.len();
   dispatch_raw(conn, first);
   crate::utils::resp::repeat_reply(&mut conn.parser.wbuf, start, count);
}

pub(super) fn dispatch_setbit_run(conn: &mut Conn<'_>, first: &[(*const u8, usize)]) {
   let store = conn.store;
   let _write_epoch = store.data.write_epoch();
   dispatch_raw(conn, first);
   for _ in 1..256 {
      let start = conn.parser.pos;
      match conn.parser.parse_one() {
         ParseResult::Complete => {
            let raw = unsafe {
               std::slice::from_raw_parts(
                  conn.parser.parts_raw.as_ptr(),
                  conn.parser.parts_raw.len(),
               )
            };
            if raw.len() == 4 && cmd_id(unsafe { part_bytes(raw[0]) }) == CmdId::Setbit {
               dispatch_raw(conn, raw);
            } else {
               conn.parser.pos = start;
               break;
            }
         }
         ParseResult::Incomplete => break,
         ParseResult::Error => {
            conn.parser.pos = start;
            break;
         }
      }
   }
}

pub(super) fn dispatch_hget_run(conn: &mut Conn<'_>, first: &[(*const u8, usize)]) {
   let Some(key) = part_str(&mut conn.parser.wbuf, first[1]) else {
      return;
   };
   let Some(field) = part_str(&mut conn.parser.wbuf, first[2]) else {
      return;
   };
   let mut count = 1;
   while count < 256 {
      let start = conn.parser.pos;
      match conn.parser.parse_one() {
         ParseResult::Complete => {
            let next = &conn.parser.parts_raw;
            if next.len() == 3
               && cmd_id(unsafe { part_bytes(next[0]) }) == CmdId::Hget
               && unsafe { part_bytes(next[1]) } == key.as_bytes()
               && unsafe { part_bytes(next[2]) } == field.as_bytes()
            {
               count += 1;
            } else {
               conn.parser.pos = start;
               break;
            }
         }
         ParseResult::Incomplete => break,
         ParseResult::Error => {
            conn.parser.pos = start;
            break;
         }
      }
   }
   let out = &mut conn.parser.wbuf;
   let start = out.len();
   match conn.store.hget_to_buf(key, field, out) {
      Ok(true) => {}
      Ok(false) => crate::utils::resp::write_nil(out),
      Err(_) => crate::utils::resp::write_wrong_type(out),
   }
   crate::utils::resp::repeat_reply(out, start, count);
}

pub(super) fn dispatch_json_get_run(
   conn: &mut Conn<'_>,
   first_raw: &[(*const u8, usize)],
   read_only: bool,
) -> bool {
   if first_raw.len() != 3 {
      return false;
   }
   let path = unsafe { part_bytes(first_raw[2]) };
   if path != b"." {
      return false;
   }
   const BATCH: usize = 256;
   let mut keys: [&[u8]; BATCH] = [&[]; BATCH];
   let mut hashes = [0u64; BATCH];
   let mut tables = [std::ptr::null(); BATCH];
   let mut entries = [std::ptr::null(); BATCH];

   keys[0] = unsafe { part_bytes(first_raw[1]) };
   let mut count = 1;

   while count < BATCH {
      let avail = conn.parser.filled - conn.parser.pos;
      if avail < 24 {
         break;
      }
      let buf = unsafe { conn.parser.rbuf.as_ptr().add(conn.parser.pos) };
      if let Some(k_ptr) = conn.parser.try_fast_json_get_key_at(buf, avail) {
         keys[count] = unsafe { std::slice::from_raw_parts(k_ptr.0, k_ptr.1) };
         count += 1;
      } else {
         break;
      }
   }

   if count == 1 || keys[1..count].iter().all(|key| *key == keys[0]) {
      let out = &mut conn.parser.wbuf;
      let start = out.len();
      let Some(key) = part_str(out, first_raw[1]) else {
         crate::utils::resp::repeat_reply(out, start, count);
         return true;
      };
      match conn.store.json_get_to_buf(key, &["."], out) {
         Ok(true) => {}
         Ok(false) => crate::utils::resp::write_nil(out),
         Err(e) => crate::utils::resp::write_store_err(out, e),
      }
      crate::utils::resp::repeat_reply(out, start, count);
      return true;
   }

   let store = conn.store;
   let read_epoch = if read_only && count >= READ_EPOCH_MIN {
      store.data.try_read_epoch()
   } else {
      None
   };

   conn
      .store
      .prefetch_slots(&keys[..count], &mut hashes[..count], &mut tables[..count]);
   if read_epoch.is_some() {
      conn.store.probe_and_prefetch_entries(
         &tables[..count],
         &hashes[..count],
         &mut entries[..count],
      );
   } else {
      conn.store.probe_and_prefetch_entries_write(
         &tables[..count],
         &hashes[..count],
         &mut entries[..count],
      );
   }

   let out = &mut conn.parser.wbuf;
   out.reserve(count * 64);

   for i in 0..count {
      let p = entries[i];
      let mut found = false;
      if !p.is_null() {
         let e = unsafe { &*p.cast::<customhash::Entry<crate::storage::value::StoreValue>>() };
         let state = e.state.load(std::sync::atomic::Ordering::Relaxed);
         let expected = ((hashes[i] as u32 as u64) << 32) | customhash::STATE_OCCUPIED;
         if (state & (0xFFFF_FFFF_0000_0000 | customhash::STATE_OCCUPIED)) == expected
            && e.key.eq_bytes(keys[i])
         {
            let read = if read_epoch.is_some() && state & customhash::STATE_LOCK == 0 {
               None
            } else {
               let Some(read) = e.read() else {
                  crate::utils::resp::write_nil(out);
                  continue;
               };
               Some(read)
            };
            let val = match &read {
               Some(read) => &**read,
               None => unsafe { (*e.value.get()).assume_init_ref() },
            };
            if val.expires_ms == 0 || !val.is_expired() {
               if let Some(raw) = val.value.as_json_str() {
                  crate::utils::resp::write_bulk(out, raw);
                  found = true;
               }
            }
         }
      }
      if !found {
         let Ok(key_str) = std::str::from_utf8(keys[i]) else {
            crate::utils::resp::write_err(out, "invalid UTF-8 in request");
            continue;
         };
         match conn.store.json_get_to_buf(key_str, &["."], out) {
            Ok(true) => {}
            Ok(false) => crate::utils::resp::write_nil(out),
            Err(e) => crate::utils::resp::write_store_err(out, e),
         }
      }
   }
   true
}

#[inline]
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

   if matches!(conn.mode, ConnMode::Subscribed { .. }) {
      if id == CmdId::Ping {
         if raw.len() > 2 {
            crate::utils::resp::write_wrong_args(&mut conn.parser.wbuf, "ping");
         } else {
            conn.parser.wbuf.extend_from_slice(b"*2\r\n$4\r\npong\r\n");
            let message = raw
               .get(1)
               .map_or(&[][..], |part| unsafe { part_bytes(*part) });
            crate::utils::resp::write_bulk_bytes(&mut conn.parser.wbuf, message);
         }
      } else if [
         b"SUBSCRIBE".as_slice(),
         b"UNSUBSCRIBE",
         b"PSUBSCRIBE",
         b"PUNSUBSCRIBE",
         b"ASKING",
         b"RESET",
         b"QUIT",
      ]
      .iter()
      .any(|allowed| cmd.eq_ignore_ascii_case(allowed))
      {
         dispatch_text(conn, raw);
      } else {
         crate::utils::resp::write_err(
            &mut conn.parser.wbuf,
            "Command not allowed in subscribed state",
         );
      }
      return;
   }

   if id == CmdId::Auth {
      handle_auth(conn, raw);
      return;
   } else if id == CmdId::Hello {
      handle_hello(conn, raw);
      return;
   }

   if !conn.mode.executing_transaction()
      && !conn.store.cluster.enabled
      && !conn.store.at_key_capacity()
   {
      match id {
         CmdId::Get if raw.len() == 2 => {
            dispatch_get_run::<false>(conn, raw[1], false, &customhash::pin());
            return;
         }
         CmdId::Set if raw.len() == 3 => {
            dispatch_set_run(conn, raw[1], raw[2], &customhash::pin());
            return;
         }
         CmdId::Del if raw.len() == 2 => {
            let key = unsafe { part_bytes(raw[1]) };
            crate::utils::resp::write_boolean(&mut conn.parser.wbuf, conn.store.del_bytes(key));
            return;
         }
         CmdId::Hget if raw.len() == 3 => {
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
         }
         CmdId::Hset if raw.len() == 4 => {
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
         }
         CmdId::Ping => {
            if raw.len() == 1 {
               conn.parser.wbuf.extend_from_slice(crate::utils::resp::PONG);
               return;
            } else if raw.len() == 2 {
               let msg = unsafe { part_bytes(raw[1]) };
               crate::utils::resp::write_bulk_bytes(&mut conn.parser.wbuf, msg);
               return;
            }
         }
         CmdId::Incr if raw.len() == 2 => {
            let out = &mut conn.parser.wbuf;
            let key = unsafe { part_bytes(raw[1]) };
            match conn.store.incr_bytes(key) {
               Ok(n) => {
                  if conn.store.has_replication() {
                     conn.store.record_current_value_bytes(key);
                  }
                  crate::utils::resp::write_integer(out, n)
               }
               Err(e) => crate::utils::resp::write_err(out, e),
            }
            return;
         }
         CmdId::Rpop if raw.len() == 2 => {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let popped = match conn
               .store
               .pop_one_to_scratch(key, true, &mut conn.pop_scratch)
            {
               Ok(popped) => popped,
               Err(_) => {
                  crate::utils::resp::write_wrong_type(out);
                  return;
               }
            };
            if popped {
               crate::utils::resp::write_bulk_bytes(out, conn.pop_scratch.as_bytes());
               if conn.store.has_replication() {
                  conn.store.record_current_value(key);
               }
            } else {
               out.extend_from_slice(b"$-1\r\n");
            }
            return;
         }
         CmdId::Lpop if raw.len() == 2 => {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let popped = match conn
               .store
               .pop_one_to_scratch(key, false, &mut conn.pop_scratch)
            {
               Ok(popped) => popped,
               Err(_) => {
                  crate::utils::resp::write_wrong_type(out);
                  return;
               }
            };
            if popped {
               crate::utils::resp::write_bulk_bytes(out, conn.pop_scratch.as_bytes());
               if conn.store.has_replication() {
                  conn.store.record_current_value(key);
               }
            } else {
               out.extend_from_slice(b"$-1\r\n");
            }
            return;
         }
         CmdId::Sadd if raw.len() == 3 => {
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
         }
         CmdId::Zadd if raw.len() == 4 => {
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
         }
         CmdId::Xadd if raw.len() == 5 => {
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
         CmdId::Lpush if raw.len() == 3 => {
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
         }
         CmdId::Rpush if raw.len() == 3 => {
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
         }
         CmdId::Pfadd if raw.len() == 3 => {
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
         CmdId::Expire if raw.len() == 3 => {
            let out = &mut conn.parser.wbuf;
            let key = unsafe { part_bytes(raw[1]) };
            let Some(secs) = part_str(out, raw[2]) else {
               return;
            };
            let reply = match secs
               .parse::<i64>()
               .ok()
               .and_then(crate::storage::value::expiry_from_secs_signed)
            {
               Some(exp) => {
                  let ok = conn.store.expire_ms_bytes(key, exp);
                  if conn.store.has_replication() {
                     conn.store.record_current_value_bytes(key);
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
         }
         CmdId::Setbit if raw.len() == 4 => {
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
         }
         CmdId::Geoadd if raw.len() == 5 => {
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
         CmdId::Pfcount if raw.len() == 2 => {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            match conn.store.pfcount(&[key]) {
               Ok(n) => crate::utils::resp::write_integer(out, n as i64),
               Err(e) => crate::utils::resp::write_store_err(out, e),
            }
            return;
         }
         CmdId::Geodist if raw.len() == 4 => {
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
               Ok(Some(d)) => crate::utils::resp::write_float(out, d),
               Ok(None) => crate::utils::resp::write_nil(out),
               Err(e) => crate::utils::resp::write_store_err(out, e),
            }
            return;
         }
         CmdId::Publish if raw.len() == 3 => {
            let out = &mut conn.parser.wbuf;
            let Some(channel) = part_str(out, raw[1]) else {
               return;
            };
            let message = unsafe { part_bytes(raw[2]) };
            let n = conn.pubsub.publish_bytes(channel, message);
            crate::utils::resp::write_integer(out, n as i64);
            return;
         }
         CmdId::Bitcount if raw.len() == 2 => {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            match conn.store.bitcount(key, 0, -1, false) {
               Ok(c) => crate::utils::resp::write_integer(out, c as i64),
               Err(_) => crate::utils::resp::write_wrong_type(out),
            }
            return;
         }
         CmdId::JsonSet if raw.len() == 4 => {
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
         }
         CmdId::JsonGet if raw.len() >= 2 => {
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
         _ => {}
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

   if conn.store.cluster.enabled && !cluster_admits(conn, id, cmd, raw, cluster_is_write) {
      return;
   }

   if !conn.mode.executing_transaction() && park::is_blocking_cmd(cmd) {
      park::dispatch_blocking(conn, raw);
      return;
   }
   if !conn.mode.executing_transaction() && park::is_blocking_stream_read(cmd, raw) {
      park::dispatch_stream_read(conn, raw);
      return;
   }

   match id {
      CmdId::Set if raw.len() >= 3 => {
         let (hint, entry) = conn.hint_for(raw[1]);
         let out = &mut conn.parser.wbuf;
         let Some(key) = part_str(out, raw[1]) else {
            return;
         };
         if raw.len() == 3 {
            let bytes = unsafe { part_bytes(raw[2]) };
            conn
               .store
               .set_string_auto_bytes(key.as_bytes(), hint, entry, bytes, 0);
            if conn.store.has_replication() {
               conn.store.record_current_value_bytes(key.as_bytes());
            }
            conn.parser.wbuf.extend_from_slice(b"+OK\r\n");
            return;
         }
      }
      CmdId::Get if raw.len() == 2 => {
         let (hint, entry) = conn.hint_for(raw[1]);
         let out = &mut conn.parser.wbuf;
         let Some(key) = part_str(out, raw[1]) else {
            return;
         };
         if !conn.store.get_to_buf_auto(key, hint, entry, out) {
            out.extend_from_slice(b"$-1\r\n");
         }
         return;
      }
      CmdId::Del if raw.len() == 2 => {
         let key = unsafe { part_bytes(raw[1]) };
         if conn.store.del_bytes(key) {
            conn.parser.wbuf.extend_from_slice(b":1\r\n");
         } else {
            conn.parser.wbuf.extend_from_slice(b":0\r\n");
         }
         return;
      }
      CmdId::Hget if raw.len() == 3 => {
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
      }
      CmdId::Hset if raw.len() >= 4 && (raw.len() - 2).is_multiple_of(2) => {
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
      }
      CmdId::Ping => {
         if raw.len() == 1 {
            conn.parser.wbuf.extend_from_slice(crate::utils::resp::PONG);
            return;
         } else if raw.len() == 2 {
            let msg = unsafe { part_bytes(raw[1]) };
            crate::utils::resp::write_bulk_bytes(&mut conn.parser.wbuf, msg);
            return;
         }
      }
      CmdId::Incr if raw.len() == 2 => {
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
      }
      CmdId::Rpop if raw.len() == 2 => {
         let out = &mut conn.parser.wbuf;
         let Some(key) = part_str(out, raw[1]) else {
            return;
         };
         let store = conn.store;
         let popped = match store.pop_one_to_scratch(key, true, &mut conn.pop_scratch) {
            Ok(popped) => popped,
            Err(_) => {
               crate::utils::resp::write_wrong_type(&mut conn.parser.wbuf);
               return;
            }
         };
         if popped {
            crate::utils::resp::write_bulk_bytes(
               &mut conn.parser.wbuf,
               conn.pop_scratch.as_bytes(),
            );
            if store.has_replication() {
               store.record_current_value(key);
            }
         } else {
            conn.parser.wbuf.extend_from_slice(b"$-1\r\n");
         }
         return;
      }
      CmdId::Lpop if raw.len() == 2 => {
         let out = &mut conn.parser.wbuf;
         let Some(key) = part_str(out, raw[1]) else {
            return;
         };
         let store = conn.store;
         let popped = match store.pop_one_to_scratch(key, false, &mut conn.pop_scratch) {
            Ok(popped) => popped,
            Err(_) => {
               crate::utils::resp::write_wrong_type(&mut conn.parser.wbuf);
               return;
            }
         };
         if popped {
            crate::utils::resp::write_bulk_bytes(
               &mut conn.parser.wbuf,
               conn.pop_scratch.as_bytes(),
            );
            if store.has_replication() {
               store.record_current_value(key);
            }
         } else {
            conn.parser.wbuf.extend_from_slice(b"$-1\r\n");
         }
         return;
      }
      CmdId::Sadd if raw.len() >= 3 => {
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
      CmdId::Expire if raw.len() == 3 => {
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
         conn.parser.wbuf.extend_from_slice(reply);
         return;
      }
      CmdId::Lpush if raw.len() == 3 => {
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
      _ => {}
   }
   if cmd_len == 9
      && (cmd.eq_ignore_ascii_case(b"FT.SEARCH") || cmd.eq_ignore_ascii_case(b"FT.HYBRID"))
      && raw.len() >= 3
   {
      return ft_search_raw(conn, raw);
   }
   if !conn.mode.executing_transaction() && is_offloaded_ft(cmd) {
      return ft_offload_generic(conn, raw);
   }

   dispatch_text(conn, raw);
}

fn dispatch_text(conn: &mut Conn<'_>, raw: &[(*const u8, usize)]) {
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

#[inline]
fn cluster_admits(
   conn: &mut Conn<'_>,
   id: CmdId,
   cmd: &[u8],
   raw: &[(*const u8, usize)],
   cluster_is_write: bool,
) -> bool {
   if conn.store.replica_installing() {
      conn
         .parser
         .wbuf
         .extend_from_slice(b"-CLUSTERDOWN Replica snapshot is installing\r\n");
      return false;
   }
   conn.refresh_topology();
   if cluster_is_write && !conn.local_primary {
      conn
         .parser
         .wbuf
         .extend_from_slice(b"-READONLY You can't write against a read only replica.\r\n");
      return false;
   }

   let asking = std::mem::take(&mut conn.asking);

   // The argument slice is materialized only for multi-key commands;
   // keyless and single-key commands skip it.
   let scope = match id {
      CmdId::Get | CmdId::Set => crate::cluster::RoutingScope::FirstKey,
      _ => crate::cluster::routing_scope(cmd),
   };
   let decision = match scope {
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
         let topology = conn
            .topology_cache
            .as_ref()
            .map(|(_, topology, _)| topology.as_ref());
         write_route_decision(&mut conn.parser.wbuf, other, topology);
         return false;
      }
   }
   true
}

pub(super) fn dispatch_cluster_strings<const SET: bool, const STREAM_IO: bool>(
   conn: &mut Conn<'_>,
   first: &[(*const u8, usize)],
   pin: &customhash::Guard,
) {
   let store = conn.store;
   let _ticket = SET.then(|| store.begin_write(conn.worker));
   let (id, cmd, header) = if SET {
      (CmdId::Set, b"SET", b"*3\r\n")
   } else {
      (CmdId::Get, b"GET", b"*2\r\n")
   };
   let mut parts = [
      first[0],
      first[1],
      first.get(2).copied().unwrap_or(first[1]),
   ];
   let mut writer = None;
   let mut view = None;
   let read_limit = conn
      .parser
      .wbuf
      .len()
      .saturating_add(super::bulk::DIRECT_SEND_MIN);
   for index in 0..64 {
      if index != 0 {
         let pending = &conn.parser.rbuf[conn.parser.pos..conn.parser.filled];
         if !pending.starts_with(header) {
            break;
         }
         let (ptr, len) = (pending.as_ptr(), pending.len());
         if SET {
            let Some((key, value)) = conn.parser.try_fast_set_key_val_at(ptr, len) else {
               break;
            };
            parts[1] = key;
            parts[2] = value;
         } else {
            let Some(key) = conn.parser.try_fast_get_key_at(ptr, len) else {
               break;
            };
            parts[1] = key;
         }
      }
      if SET && store.at_key_capacity() {
         conn
            .parser
            .wbuf
            .extend_from_slice(crate::storage::capacity::OOM_REPLY);
         continue;
      }
      if !cluster_admits(conn, id, cmd, &parts[..first.len()], SET) {
         continue;
      }
      let Some(key) = part_str(&mut conn.parser.wbuf, parts[1]) else {
         continue;
      };
      if SET {
         if index == 1 && !store.has_replication() {
            writer = Some(store.data.write_epoch());
         }
         dispatch_set_one(conn, parts[1], parts[2], pin);
      } else {
         if index == 1 && conn.parser.wbuf.len() < read_limit {
            view = store.data.try_read_view();
         }
         if let Some(batch) = &view {
            match batch.get(key) {
               None => {
                  crate::utils::resp::write_nil(&mut conn.parser.wbuf);
                  continue;
               }
               Some(v) if !v.is_expired() => match v.value.as_string() {
                  None => {
                     crate::utils::resp::write_nil(&mut conn.parser.wbuf);
                     continue;
                  }
                  Some(s) if s.len() < read_limit.saturating_sub(conn.parser.wbuf.len()) => {
                     s.write_resp(&mut conn.parser.wbuf);
                     continue;
                  }
                  _ => {}
               },
               _ => {}
            }
            // Release writer exclusion before expiry cleanup or copying a large reply.
            drop(view.take());
         }
         dispatch_get_one::<STREAM_IO>(conn, parts[1], pin);
      }
   }
   drop(writer);
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
pub(super) fn write_route_decision(
   out: &mut Vec<u8>,
   decision: crate::cluster::RouteDecision<'_>,
   topology: Option<&crate::cluster::Topology>,
) {
   #[inline]
   fn write_redirect(out: &mut Vec<u8>, kind: &[u8], slot: crate::cluster::Slot, address: &str) {
      out.extend_from_slice(kind);
      crate::utils::resp::write_usize(out, slot.value() as usize);
      out.push(b' ');
      out.extend_from_slice(address.as_bytes());
      out.extend_from_slice(b"\r\n");
   }
   match decision {
      crate::cluster::RouteDecision::Moved { slot, address } => {
         write_redirect(out, b"-MOVED ", slot, address);
      }
      crate::cluster::RouteDecision::MovedNode { slot, node } => {
         if let Some(address) = topology.and_then(|topology| topology.nodes.get(node)) {
            write_redirect(out, b"-MOVED ", slot, &address.address);
         }
      }
      crate::cluster::RouteDecision::Ask { slot, node } => {
         if let Some(address) = topology.and_then(|topology| topology.nodes.get(node)) {
            write_redirect(out, b"-ASK ", slot, &address.address);
         }
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
pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
   // The length difference is folded in at full width. It used to be cast
   // to u8, so inputs whose lengths differed by a multiple of 256 passed the
   // length check and compared equal when the longer one ended in zero bytes.
   let mut diff = a.len() ^ b.len();
   for i in 0..a.len().max(b.len()) {
      let x = a.get(i).copied().unwrap_or(0);
      let y = b.get(i).copied().unwrap_or(0);
      diff |= usize::from(x ^ y);
   }
   // Keeps the optimizer from turning the loop into an early exit.
   std::hint::black_box(diff) == 0
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
   let inline = conn.mode.executing_transaction()
      || !search_offloaded()
      || (raw.len() >= 2 && {
         std::str::from_utf8(unsafe { part_bytes(raw[1]) })
            .is_ok_and(|idx| conn.store.search_state().hnsw_graph_is_clean(idx))
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
      assert_eq!(cmd_id(b"GET\0"), CmdId::Other);
      assert_eq!(cmd_id(b"SET "), CmdId::Other);
      assert_eq!(cmd_id(b"JSON\x0eGET"), CmdId::Other);
      assert_eq!(cmd_id(b"JSON\x0eSET"), CmdId::Other);
      assert_eq!(cmd_id(b"FT.SEARCH"), CmdId::Other);
   }
}

#[cfg(test)]
mod point_cache_tests {
   use super::*;

   #[test]
   fn a_new_hot_key_does_not_inherit_the_previous_keys_backoff() {
      let mut cache = PointCache {
         key: [0; 32],
         key_len: 0,
         reply: [0; 32],
         reply_len: 0,
         stamp: None,
         failures: POINT_CACHE_MAX_FAILURES - 1,
         backoff: POINT_CACHE_BACKOFF,
      };
      cache.switch_to(b"key:b");
      assert_eq!(&cache.key[..cache.key_len], b"key:b");
      assert_eq!(cache.backoff, 0);
      assert_eq!(cache.failures, 0);
      assert!(cache.stamp.is_none());
   }
}

#[cfg(test)]
mod constant_time_eq_tests {
   use super::constant_time_eq;

   #[test]
   fn equal_only_for_identical_bytes_whatever_the_length_difference() {
      assert!(constant_time_eq(b"secret", b"secret"));
      assert!(constant_time_eq(b"", b""));
      assert!(!constant_time_eq(b"secret", b"secreT"));
      assert!(!constant_time_eq(b"secret", b"secre"));
      // A length difference of 256 with zero padding used to compare equal.
      let mut padded = b"secret".to_vec();
      padded.extend(std::iter::repeat_n(0u8, 256));
      assert!(!constant_time_eq(b"secret", &padded));
      assert!(!constant_time_eq(&padded, b"secret"));
      assert!(!constant_time_eq(b"", &[0u8; 512]));
   }
}
