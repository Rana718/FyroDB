use super::*;
use crate::commands::ComdType;
use crate::storage::store::WatchedKey;

#[derive(Default)]
pub struct Transaction {
   queue: Vec<Vec<Vec<u8>>>,
   dirty: bool,
   active: bool,
   executing: bool,
   watches: Watches,
}

pub(super) type Watches = foldhash::HashMap<String, WatchedKey>;

#[inline]
pub(super) fn is_control(cmd: &[u8]) -> bool {
   matches!(cmd.len(), 4 | 5 | 7)
      && (cmd.eq_ignore_ascii_case(b"MULTI")
         || cmd.eq_ignore_ascii_case(b"EXEC")
         || cmd.eq_ignore_ascii_case(b"DISCARD")
         || cmd.eq_ignore_ascii_case(b"WATCH")
         || cmd.eq_ignore_ascii_case(b"UNWATCH"))
}

#[inline]
pub(super) fn is_atomic(cmd: &[u8]) -> bool {
   cmd.eq_ignore_ascii_case(b"EVAL")
      || cmd.eq_ignore_ascii_case(b"EVALSHA")
      || cmd.eq_ignore_ascii_case(b"EXEC")
      || cmd.eq_ignore_ascii_case(b"WATCH")
}

fn session(mode: &mut ConnMode) -> &mut Transaction {
   if !matches!(mode, ConnMode::Transaction(_)) {
      *mode = ConnMode::Transaction(Box::default());
   }
   match mode {
      ConnMode::Transaction(state) => state,
      _ => unreachable!(),
   }
}

impl ConnMode {
   #[inline]
   pub(super) fn in_transaction(&self) -> bool {
      matches!(self, Self::Transaction(state) if state.active)
   }

   #[inline]
   pub(super) fn executing_transaction(&self) -> bool {
      matches!(self, Self::Transaction(state) if state.executing)
   }

   pub(super) fn clear_transaction(&mut self) {
      if matches!(self, Self::Transaction(_)) {
         *self = Self::Normal;
      }
   }
}

fn reject(conn: &mut Conn<'_>, message: &str) {
   if conn.mode.in_transaction() {
      session(&mut conn.mode).dirty = true;
   }
   crate::utils::resp::write_err(&mut conn.parser.wbuf, message);
}

pub(super) fn handle(conn: &mut Conn<'_>, raw: &[(*const u8, usize)]) {
   let cmd = unsafe { part_bytes(raw[0]) };
   if !cmd.is_ascii() {
      return reject(conn, "unknown command");
   }
   let kind = ComdType::from_bytes(cmd);
   let out = &mut conn.parser.wbuf;
   match kind {
      ComdType::MULTI => {
         if raw.len() != 1 {
            return reject(conn, "wrong number of arguments for 'multi' command");
         }
         if conn.mode.in_transaction() {
            return crate::utils::resp::write_err(out, "MULTI calls can not be nested");
         }
         session(&mut conn.mode).active = true;
         return crate::utils::resp::write_ok(out);
      }
      ComdType::DISCARD => {
         if raw.len() != 1 {
            return reject(conn, "wrong number of arguments for 'discard' command");
         }
         if !conn.mode.in_transaction() {
            return crate::utils::resp::write_err(out, "DISCARD without MULTI");
         }
         conn.mode.clear_transaction();
         return crate::utils::resp::write_ok(out);
      }
      ComdType::WATCH => {
         if raw.len() < 2 {
            return reject(conn, "wrong number of arguments for 'watch' command");
         }
         if conn.mode.in_transaction() {
            return crate::utils::resp::write_err(out, "WATCH inside MULTI is not allowed");
         }
         let _guard = conn.store.atomic_command_guard();
         let mut keys = Vec::with_capacity(raw.len() - 1);
         for &part in &raw[1..] {
            let Some(key) = part_str(out, part) else {
               return;
            };
            keys.push(key);
         }
         let watches = &mut session(&mut conn.mode).watches;
         for key in keys {
            watches
               .entry(key.to_owned())
               .or_insert_with(|| conn.store.watch_key(key));
         }
         return crate::utils::resp::write_ok(out);
      }
      ComdType::UNWATCH if !conn.mode.in_transaction() => {
         if raw.len() != 1 {
            return reject(conn, "wrong number of arguments for 'unwatch' command");
         }
         conn.mode.clear_transaction();
         return crate::utils::resp::write_ok(out);
      }
      ComdType::Exec if cmd.eq_ignore_ascii_case(b"EXEC") => {
         if raw.len() != 1 {
            return reject(conn, "wrong number of arguments for 'exec' command");
         }
         if !conn.mode.in_transaction() {
            return crate::utils::resp::write_err(out, "EXEC without MULTI");
         }
         let ConnMode::Transaction(mut tx) = std::mem::replace(&mut conn.mode, ConnMode::Normal)
         else {
            unreachable!()
         };
         let _guard = conn.store.atomic_command_guard();
         if tx.dirty {
            return out.extend_from_slice(
               b"-EXECABORT Transaction discarded because of previous errors.\r\n",
            );
         }
         if tx.watches.values().any(WatchedKey::changed) {
            return out.extend_from_slice(b"*-1\r\n");
         }
         tx.watches.clear();
         let queue = std::mem::take(&mut tx.queue);
         crate::utils::resp::write_array_header(out, queue.len());
         tx.active = false;
         tx.executing = true;
         conn.mode = ConnMode::Transaction(tx);
         conn.hint_n = 0;
         for parts in queue {
            let raw: Vec<_> = parts.iter().map(|p| (p.as_ptr(), p.len())).collect();
            if parts[0].eq_ignore_ascii_case(b"UNWATCH") {
               crate::utils::resp::write_ok(&mut conn.parser.wbuf);
            } else {
               dispatch_raw(conn, &raw);
            }
         }
         conn.mode.clear_transaction();
         return;
      }
      _ => {}
   }
   let publish = cmd.eq_ignore_ascii_case(b"PUBLISH");
   if !publish && !kind.as_str().as_bytes().eq_ignore_ascii_case(cmd) {
      return reject(conn, "unknown command");
   }
   if (publish && raw.len() != 3) || (!publish && !valid_arity(kind, raw.len())) {
      return reject(
         conn,
         &format!(
            "wrong number of arguments for '{}' command",
            String::from_utf8_lossy(cmd).to_ascii_lowercase()
         ),
      );
   }
   if conn.store.at_key_capacity() && crate::storage::capacity::is_denyoom_command(cmd) {
      session(&mut conn.mode).dirty = true;
      return out.extend_from_slice(crate::storage::capacity::OOM_REPLY);
   }
   session(&mut conn.mode).queue.push(
      raw.iter()
         .map(|&p| unsafe { part_bytes(p) }.to_vec())
         .collect(),
   );
   out.extend_from_slice(b"+QUEUED\r\n");
}

fn valid_arity(kind: ComdType, count: usize) -> bool {
   use ComdType::*;
   let arity: isize = match kind {
      ACL => 0,
      Exec | FLUSH | DBSIZE | LASTSAVE | TIME | QUIT | RESET | RANDOMKEY | MULTI | DISCARD
      | UNWATCH | FTALIASLIST | FTALIST => 1,
      ECHO | TYPE | SELECT | GET | GETDEL | INCR | DECR | STRLEN | TTL | PTTL | EXPIRETIME
      | PEXPIRETIME | PERSIST | KEYS | HGETALL | HLEN | HKEYS | HVALS | LLEN | SMEMBERS | SCARD
      | ZCARD | XLEN | MODULE | FTALIASDEL => 2,
      SETNX | GETSET | INCRBY | DECRBY | INCRBYFLOAT | APPEND | RENAME | RENAMENX | HGET
      | HEXISTS | LINDEX | RPOPLPUSH | SISMEMBER | ZSCORE | GETBIT | JSONTOGGLE | FTALIASADD
      | FTALIASUPDATE => 3,
      SETEX | PSETEX | GETRANGE | SETRANGE | SUBSTR | HSETNX | HINCRBY | HINCRBYFLOAT | LSET
      | LRANGE | LTRIM | LREM | SMOVE | ZCOUNT | ZLEXCOUNT | ZINCRBY | SETBIT | JSONNUMINCRBY
      | JSONNUMMULTBY => 4,
      LINSERT | LMOVE | JSONARRTRIM => 5,
      BLMOVE => 6,
      PING | INFO | BGSAVE | SAVE | COMMAND | HELLO | FLUSHALL | FLUSHDB => -1,
      DBCACHE | PURGE | CLUSTER | AUTH | CLIENT | CONFIG | SLOWLOG | OBJECT | SORT | GETEX
      | MGET | DEL | UNLINK | EXISTS | SCAN | TOUCH | HRANDFIELD | LPOP | RPOP | SPOP
      | SRANDMEMBER | SUNION | SINTER | SDIFF | ZPOPMIN | ZPOPMAX | ZRANDMEMBER | BITCOUNT
      | BITFIELD | PFCOUNT | PFMERGE | JSONGET | JSONDEL | JSONFORGET | JSONTYPE | JSONSTRLEN
      | JSONARRLEN | JSONARRPOP | JSONOBJKEYS | JSONOBJLEN | JSONCLEAR | JSONRESP | GEOPOS
      | GEOHASH | XINFO | WATCH | FTCREATE | FTINFO | FTDROPINDEX | SCRIPT => -2,
      SET | LCS | EXPIRE | PEXPIRE | PEXPIREAT | EXPIREAT | COPY | HMGET | HDEL | HSCAN | LPUSH | RPUSH
      | LPOS | BLPOP | BRPOP | SADD | SREM | SMISMEMBER | SUNIONSTORE | SINTERSTORE
      | SDIFFSTORE | SINTERCARD | SSCAN | ZREM | ZMSCORE | ZRANK | ZREVRANK | BZPOPMIN
      | BZPOPMAX | ZUNION | ZINTER | ZDIFF | ZSCAN | BITPOS | PFADD | JSONSTRAPPEND | JSONMGET
      | XREAD | XDEL | XGROUP | XPENDING | FTALTER | FTSEARCH | FTAGGREGATE | FTCURSOR
      | FTCONFIG | EVAL | EVALSHA => -3,
      ZADD | ZRANGE | ZREVRANGE | ZRANGEBYSCORE | ZREVRANGEBYSCORE | ZRANGEBYLEX | ZUNIONSTORE
      | ZINTERSTORE | ZDIFFSTORE | BITOP | JSONSET | JSONARRAPPEND | JSONARRINDEX | GEODIST
      | XRANGE | XREVRANGE | XTRIM | XACK => -4,
      ZRANGESTORE | JSONARRINSERT | GEOADD | XADD => -5,
      XREADGROUP | XCLAIM | XAUTOCLAIM => -6,
      GEOSEARCH => -7,
      GEOSEARCHSTORE => -8,
      HSET | HMSET => return count >= 4 && count.is_multiple_of(2),
      MSET | MSETNX => return count >= 3 && count % 2 == 1,
   };
   if arity > 0 {
      count == arity as usize
   } else {
      count >= (-arity) as usize
   }
}
