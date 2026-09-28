use crate::parse_int;
use crate::storage::store::Store;
use crate::storage::value::{expiry_from_ms, expiry_from_secs, expiry_from_unix_secs};
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

pub fn expire(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, secs] = parts else {
        return resp::write_wrong_args(out, "expire");
    };
    let s = parse_int!(out, secs, u64);
    let Some(exp) = expiry_from_secs(s) else {
        return resp::write_err(out, "invalid expire time in 'expire' command");
    };
    resp::write_boolean(out, store.expire_ms(key, exp));
}

pub fn pexpire(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, ms] = parts else {
        return resp::write_wrong_args(out, "pexpire");
    };
    let m = parse_int!(out, ms, u64);
    let Some(exp) = expiry_from_ms(m) else {
        return resp::write_err(out, "invalid expire time in 'pexpire' command");
    };
    resp::write_boolean(out, store.expire_ms(key, exp));
}

pub fn expireat(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, ts] = parts else {
        return resp::write_wrong_args(out, "expireat");
    };
    let unix_secs = parse_int!(out, ts, u64);
    let Some(exp) = expiry_from_unix_secs(unix_secs) else {
        return resp::write_err(out, "invalid expire time in 'expireat' command");
    };
    resp::write_boolean(out, store.expire_ms(key, exp));
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
    let all = store.keys_matching(pattern);
    resp::write_array(out, &all);
}

pub fn expiretime(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key] = parts else {
        return resp::write_wrong_args(out, "expiretime");
    };
    match store.data.get_ref(key) {
        None => resp::write_integer(out, -2),
        Some(e) if e.is_expired() => resp::write_integer(out, -2),
        Some(e) => {
            if e.expires_ms == 0 {
                resp::write_integer(out, -1);
            } else {
                resp::write_integer(out, (e.expires_ms / 1000) as i64);
            }
        }
    }
}

pub fn pexpiretime(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key] = parts else {
        return resp::write_wrong_args(out, "pexpiretime");
    };
    match store.data.get_ref(key) {
        None => resp::write_integer(out, -2),
        Some(e) if e.is_expired() => resp::write_integer(out, -2),
        Some(e) => {
            if e.expires_ms == 0 {
                resp::write_integer(out, -1);
            } else {
                resp::write_integer(out, e.expires_ms as i64);
            }
        }
    }
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
