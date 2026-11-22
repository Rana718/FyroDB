use crate::storage::store::Store;
use crate::utils::resp;
use crate::{parse_int, store_ok, wt};

#[inline]
fn set_op_members(
   parts: &[&str],
   out: &mut Vec<u8>,
   cmd: &'static str,
   op: impl FnOnce(&str, &[&str]) -> Result<usize, &'static str>,
) {
   match parts {
      [_, key, members @ ..] if !members.is_empty() => {
         resp::write_integer(out, wt!(out, op(key, members)) as i64);
      }
      _ => resp::write_wrong_args(out, cmd),
   }
}

pub fn sadd(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   set_op_members(parts, out, "sadd", |k, m| store.sadd(k, m));
}

pub fn srem(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   set_op_members(parts, out, "srem", |k, m| store.srem(k, m));
}

pub fn sismember(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   match parts {
      [_, key, member] => resp::write_boolean(out, wt!(out, store.sismember(key, member))),
      _ => resp::write_wrong_args(out, "sismember"),
   }
}

pub fn smismember(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   match parts {
      [_, key, members @ ..] if !members.is_empty() => {
         wt!(out, store.smismember_to_buf(key, members, out));
      }
      _ => resp::write_wrong_args(out, "smismember"),
   }
}

pub fn smembers(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let [_, key] = parts else {
      return resp::write_wrong_args(out, "smembers");
   };
   wt!(out, store.smembers_to_buf(key, out));
}

pub fn scard(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   match parts {
      [_, key] => resp::write_integer(out, wt!(out, store.scard(key)) as i64),
      _ => resp::write_wrong_args(out, "scard"),
   }
}

pub fn spop(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   if let [_, key] = parts {
      return match wt!(out, store.spop_one_to_buf(key, out)) {
         true => {}
         false => resp::write_nil(out),
      };
   }
   let [_, key, cnt] = parts else {
      return resp::write_wrong_args(out, "spop");
   };
   let count = parse_int!(out, cnt, usize);
   let items = wt!(out, store.spop(key, count));
   resp::write_array(out, &items);
}

pub fn srandmember(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let (key, count, single) = match parts {
      [_, key] => (*key, 1i64, true),
      [_, key, cnt] => {
         let c = parse_int!(out, cnt);
         (*key, c, false)
      }
      _ => return resp::write_wrong_args(out, "srandmember"),
   };
   wt!(out, store.srandmember_to_buf(key, count, single, out));
}

pub fn smove(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   match parts {
      [_, src, dst, member] => resp::write_boolean(out, wt!(out, store.smove(src, dst, member))),
      _ => resp::write_wrong_args(out, "smove"),
   }
}

pub fn sunion(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   match parts {
      [_, keys @ ..] if !keys.is_empty() => wt!(out, store.sunion_to_buf(keys, out)),
      _ => resp::write_wrong_args(out, "sunion"),
   }
}

pub fn sinter(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   match parts {
      [_, keys @ ..] if !keys.is_empty() => wt!(out, store.sinter_to_buf(keys, out)),
      _ => resp::write_wrong_args(out, "sinter"),
   }
}

pub fn sdiff(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   match parts {
      [_, keys @ ..] if !keys.is_empty() => wt!(out, store.sdiff_to_buf(keys, out)),
      _ => resp::write_wrong_args(out, "sdiff"),
   }
}

#[inline]
fn set_store_op(
   parts: &[&str],
   out: &mut Vec<u8>,
   cmd: &'static str,
   op: impl FnOnce(&str, &[&str]) -> Result<usize, &'static str>,
) {
   match parts {
      [_, dst, keys @ ..] if !keys.is_empty() => {
         resp::write_integer(out, store_ok!(out, op(dst, keys)) as i64);
      }
      _ => resp::write_wrong_args(out, cmd),
   }
}

pub fn sunionstore(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   set_store_op(parts, out, "sunionstore", |dst, k| {
      store.sunionstore(dst, k)
   });
}

pub fn sinterstore(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   set_store_op(parts, out, "sinterstore", |dst, k| {
      store.sinterstore(dst, k)
   });
}

pub fn sdiffstore(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   set_store_op(parts, out, "sdiffstore", |dst, k| store.sdiffstore(dst, k));
}

pub fn sintercard(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let [_, numkeys_str, rest @ ..] = parts else {
      return resp::write_wrong_args(out, "sintercard");
   };
   let numkeys = parse_int!(out, numkeys_str, usize);
   if rest.len() < numkeys {
      return resp::write_wrong_args(out, "sintercard");
   }
   let keys = &rest[..numkeys];
   let mut limit = 0usize;
   let remaining = &rest[numkeys..];
   let mut i = 0;
   while i < remaining.len() {
      if remaining[i].eq_ignore_ascii_case("LIMIT") {
         i += 1;
         if i >= remaining.len() {
            return resp::write_err(out, "syntax error");
         }
         limit = parse_int!(out, remaining[i], usize);
      } else {
         return resp::write_err(out, "syntax error");
      }
      i += 1;
   }
   resp::write_integer(out, store_ok!(out, store.sintercard(keys, limit)) as i64);
}

pub fn sscan(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let [_, key, cursor_str, rest @ ..] = parts else {
      return resp::write_wrong_args(out, "sscan");
   };
   let _cursor = parse_int!(out, cursor_str, usize);
   let mut pattern: Option<&str> = None;
   let mut i = 0;
   while i < rest.len() {
      if rest[i].eq_ignore_ascii_case("MATCH") {
         i += 1;
         if i >= rest.len() {
            return resp::write_err(out, "syntax error");
         }
         pattern = Some(rest[i]);
      } else if rest[i].eq_ignore_ascii_case("COUNT") {
         i += 1;
         if i >= rest.len() {
            return resp::write_err(out, "syntax error");
         }
         let _ = parse_int!(out, rest[i], usize);
      } else {
         return resp::write_err(out, "syntax error");
      }
      i += 1;
   }

   let Some(e) = store.data.get_ref(key) else {
      return resp::write_scan_cursor_header(out, "0", 0);
   };
   if e.is_expired() {
      return resp::write_scan_cursor_header(out, "0", 0);
   }
   match e.value.as_set() {
      Some(s) => {
         if pattern.is_none() {
            resp::write_scan_cursor_header(out, "0", s.len());
            for m in s.iter() {
               m.write_bulk_to(out);
            }
            return;
         }
         let pat = pattern.unwrap();
         let mut matching = Vec::new();
         for m in s.iter() {
            let matches = match m {
               crate::storage::value::SetMemberRef::String(str_ref) => {
                  crate::utils::util::glob_match(pat, str_ref.as_str())
               }
               crate::storage::value::SetMemberRef::Integer(n) => {
                  let mut buf = [0u8; 20];
                  let str_n = crate::utils::int_format::write_i64_to(&mut buf, n);
                  crate::utils::util::glob_match(pat, str_n)
               }
            };
            if matches {
               matching.push(m);
            }
         }
         resp::write_scan_cursor_header(out, "0", matching.len());
         for m in matching {
            m.write_bulk_to(out);
         }
      }
      None => resp::write_wrong_type(out),
   }
}
