use crate::parse_int;
use crate::storage::store::Store;
use crate::storage::value::{
   expiry_from_ms_signed, expiry_from_secs_signed, expiry_from_unix_ms, expiry_from_unix_secs,
};
use crate::utils::resp;

pub fn del(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   match parts {
      [_, keys @ ..] if !keys.is_empty() => {
         resp::write_integer(out, keys.iter().filter(|k| store.del(k)).count() as i64)
      }
      _ => resp::write_wrong_args(out, "del"),
   }
}

pub fn unlink(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   del(parts, store, out);
}

pub fn exists_check(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   match parts {
      [_, keys @ ..] if !keys.is_empty() => {
         resp::write_integer(out, keys.iter().filter(|k| store.exists(k)).count() as i64)
      }
      _ => resp::write_wrong_args(out, "exists"),
   }
}

pub fn ttl_check(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let [_, key] = parts else {
      return resp::write_wrong_args(out, "ttl");
   };
   let ttl_ms = store.ttl_value_ms(key);
   resp::write_integer(out, if ttl_ms >= 0 { ttl_ms / 1000 } else { ttl_ms });
}

pub fn pttl_check(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let [_, key] = parts else {
      return resp::write_wrong_args(out, "pttl");
   };
   resp::write_integer(out, store.ttl_value_ms(key));
}

#[inline]
fn run_expire_signed(
   parts: &[&str],
   store: &Store,
   out: &mut Vec<u8>,
   cmd: &'static str,
   calc: fn(i64) -> Option<u64>,
   err_msg: &'static str,
) {
   let [_, key, val_str] = parts else {
      return resp::write_wrong_args(out, cmd);
   };
   let val = parse_int!(out, val_str, i64);
   let Some(exp) = calc(val) else {
      return resp::write_err(out, err_msg);
   };
   resp::write_boolean(out, store.expire_ms(key, exp));
}

#[inline]
fn run_expire_unsigned(
   parts: &[&str],
   store: &Store,
   out: &mut Vec<u8>,
   cmd: &'static str,
   calc: fn(u64) -> Option<u64>,
   err_msg: &'static str,
) {
   let [_, key, val_str] = parts else {
      return resp::write_wrong_args(out, cmd);
   };
   let val = parse_int!(out, val_str, u64);
   let Some(exp) = calc(val) else {
      return resp::write_err(out, err_msg);
   };
   resp::write_boolean(out, store.expire_ms(key, exp));
}

pub fn expire(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   run_expire_signed(
      parts,
      store,
      out,
      "expire",
      expiry_from_secs_signed,
      "invalid expire time in 'expire' command",
   );
}

pub fn pexpire(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   run_expire_signed(
      parts,
      store,
      out,
      "pexpire",
      expiry_from_ms_signed,
      "invalid expire time in 'pexpire' command",
   );
}

pub fn pexpireat(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   run_expire_unsigned(
      parts,
      store,
      out,
      "pexpireat",
      expiry_from_unix_ms,
      "invalid expire time in 'pexpireat' command",
   );
}

pub fn expireat(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   run_expire_unsigned(
      parts,
      store,
      out,
      "expireat",
      expiry_from_unix_secs,
      "invalid expire time in 'expireat' command",
   );
}

pub fn persist(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   match parts {
      [_, key] => resp::write_boolean(out, store.persist(key)),
      _ => resp::write_wrong_args(out, "persist"),
   }
}

pub fn rename(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let [_, old, new] = parts else {
      return resp::write_wrong_args(out, "rename");
   };
   if store.rename(old, new) {
      resp::write_ok(out)
   } else {
      resp::write_err(out, "no such key")
   }
}

pub fn renamenx(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   match parts {
      [_, old, new] => resp::write_boolean(out, store.renamenx(old, new)),
      _ => resp::write_wrong_args(out, "renamenx"),
   }
}

pub fn copy(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let replace = parts.iter().any(|p| p.eq_ignore_ascii_case("REPLACE"));
   match parts {
      [_, src, dst, ..] => resp::write_boolean(out, store.copy(src, dst, replace)),
      _ => resp::write_wrong_args(out, "copy"),
   }
}

pub fn randomkey(_parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   resp::write_opt_bulk(out, store.randomkey());
}

pub fn keys(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let [_, pattern] = parts else {
      return resp::write_wrong_args(out, "keys");
   };
   store.keys_matching_to_buf(pattern, out);
}

#[inline]
fn write_key_expiretime(
   parts: &[&str],
   store: &Store,
   out: &mut Vec<u8>,
   cmd: &'static str,
   in_seconds: bool,
) {
   let [_, key] = parts else {
      return resp::write_wrong_args(out, cmd);
   };
   let ans = match store.data.get_ref(key) {
      None => -2,
      Some(e) if e.is_expired() => -2,
      Some(e) => {
         if e.expires_ms == 0 {
            -1
         } else if in_seconds {
            (e.expires_ms / 1000) as i64
         } else {
            e.expires_ms as i64
         }
      }
   };
   resp::write_integer(out, ans);
}

pub fn expiretime(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   write_key_expiretime(parts, store, out, "expiretime", true);
}

pub fn pexpiretime(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   write_key_expiretime(parts, store, out, "pexpiretime", false);
}

pub fn touch(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   match parts {
      [_, keys @ ..] if !keys.is_empty() => {
         let count = keys.iter().filter(|k| store.exists(k)).count();
         resp::write_integer(out, count as i64);
      }
      _ => resp::write_wrong_args(out, "touch"),
   }
}
