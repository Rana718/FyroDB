use crate::storage::{
    store::Store,
    value::{SmallStr, StoreValue, expiry_from_ms, expiry_from_secs},
};
use crate::utils::resp;
use crate::utils::util::format_float;
use crate::{parse_float, parse_int, store_ok, wt};

pub fn set(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, value, rest @ ..] = parts else {
        return resp::write_wrong_args(out, "set");
    };

    let mut expires_ms: u64 = 0;
    let mut nx = false;
    let mut xx = false;
    let mut get = false;

    let mut i = 0;
    while i < rest.len() {
        let opt = rest[i].as_bytes();
        if opt.eq_ignore_ascii_case(b"EX") {
            i += 1;
            let s = parse_int!(out, rest.get(i).copied().unwrap_or(""), u64);
            if s == 0 {
                return resp::write_err(out, "invalid expire time in 'set' command");
            }
            let Some(exp) = expiry_from_secs(s) else {
                return resp::write_err(out, "invalid expire time in 'set' command");
            };
            expires_ms = exp;
        } else if opt.eq_ignore_ascii_case(b"PX") {
            i += 1;
            let ms = parse_int!(out, rest.get(i).copied().unwrap_or(""), u64);
            if ms == 0 {
                return resp::write_err(out, "invalid expire time in 'set' command");
            }
            let Some(exp) = expiry_from_ms(ms) else {
                return resp::write_err(out, "invalid expire time in 'set' command");
            };
            expires_ms = exp;
        } else if opt.eq_ignore_ascii_case(b"NX") {
            nx = true;
        } else if opt.eq_ignore_ascii_case(b"XX") {
            xx = true;
        } else if opt.eq_ignore_ascii_case(b"GET") {
            get = true;
        } else {
            resp::write_err(out, "syntax error");
            return;
        }
        i += 1;
    }

    if nx || xx || get {
        // (did_set, old value). `update_with` itself returns None when the
        // key is absent, which falls to the insert path below.
        let result = store.data.update_with(key, |current| {
            let key_exists = !current.is_expired();
            let old_val = if key_exists {
                current.value.as_string().map(|s| s.to_string())
            } else {
                None
            };

            if nx && key_exists {
                return (false, if get { old_val } else { None });
            }
            if xx && !key_exists {
                return (false, None);
            }

            current.value = crate::storage::value::FyroDB::String(SmallStr::new(value));
            current.expires_ms = expires_ms;
            (true, old_val)
        });

        match result {
            Some((true, old_val)) => {
                if get {
                    resp::write_opt_bulk(out, old_val);
                } else {
                    resp::write_ok(out);
                }
            }
            Some((false, old_val)) => {
                if get {
                    resp::write_opt_bulk(out, old_val);
                } else {
                    resp::write_nil(out);
                }
            }
            None => {
                if xx {
                    return resp::write_nil(out);
                }
                store.data.insert(
                    key.to_string(),
                    StoreValue {
                        value: crate::storage::value::FyroDB::String(SmallStr::new(value)),
                        expires_ms,
                    },
                );
                if get {
                    resp::write_nil(out);
                } else {
                    resp::write_ok(out);
                }
            }
        }
        return;
    }

    match store.try_set_string(key, value, expires_ms) {
        Ok(_) => resp::write_ok(out),
        Err(e) => resp::write_err(out, &e.to_string()),
    }
}

pub fn setnx(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key, value] => {
            resp::write_boolean(out, store.setnx(key.to_string(), value.to_string()))
        }
        _ => resp::write_wrong_args(out, "setnx"),
    }
}

pub fn setex(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, secs, value] = parts else {
        return resp::write_wrong_args(out, "setex");
    };
    let s = parse_int!(out, secs, u64);
    if s == 0 {
        return resp::write_err(out, "invalid expire time in 'setex' command");
    }
    let Some(exp) = expiry_from_secs(s) else {
        return resp::write_err(out, "invalid expire time in 'setex' command");
    };
    let sv = StoreValue {
        value: crate::storage::value::FyroDB::String(SmallStr::new(value)),
        expires_ms: exp,
    };
    match store.try_set_value(key.to_string(), sv) {
        Ok(_) => resp::write_ok(out),
        Err(e) => resp::write_err(out, &e.to_string()),
    }
}

pub fn psetex(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, ms, value] = parts else {
        return resp::write_wrong_args(out, "psetex");
    };
    let m = parse_int!(out, ms, u64);
    if m == 0 {
        return resp::write_err(out, "invalid expire time in 'psetex' command");
    }
    let Some(exp) = expiry_from_ms(m) else {
        return resp::write_err(out, "invalid expire time in 'psetex' command");
    };
    let sv = StoreValue {
        value: crate::storage::value::FyroDB::String(SmallStr::new(value)),
        expires_ms: exp,
    };
    match store.try_set_value(key.to_string(), sv) {
        Ok(_) => resp::write_ok(out),
        Err(e) => resp::write_err(out, &e.to_string()),
    }
}

pub fn get(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key] => {
            if !store.get_to_buf(key, out) {
                resp::write_nil(out);
            }
        }
        _ => resp::write_wrong_args(out, "get"),
    }
}

pub fn getdel(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key] => resp::write_opt_bulk(out, store.getdel(key)),
        _ => resp::write_wrong_args(out, "getdel"),
    }
}

pub fn getset(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key, value] => resp::write_opt_bulk(out, store.getset(key, value)),
        _ => resp::write_wrong_args(out, "getset"),
    }
}

pub fn getex(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, rest @ ..] = parts else {
        return resp::write_wrong_args(out, "getex");
    };
    if rest.is_empty() {
        return resp::write_opt_bulk(out, store.get(key));
    }
    let expires_ms: u64 = match rest {
        [opt] if opt.eq_ignore_ascii_case("PERSIST") => 0,
        [opt, secs] if opt.eq_ignore_ascii_case("EX") => {
            let s = parse_int!(out, secs, u64);
            if s == 0 {
                return resp::write_err(out, "invalid expire time in 'getex' command");
            }
            let Some(exp) = expiry_from_secs(s) else {
                return resp::write_err(out, "invalid expire time in 'getex' command");
            };
            exp
        }
        [opt, ms] if opt.eq_ignore_ascii_case("PX") => {
            let m = parse_int!(out, ms, u64);
            if m == 0 {
                return resp::write_err(out, "invalid expire time in 'getex' command");
            }
            let Some(exp) = expiry_from_ms(m) else {
                return resp::write_err(out, "invalid expire time in 'getex' command");
            };
            exp
        }
        _ => {
            resp::write_err(out, "syntax error");
            return;
        }
    };
    resp::write_opt_bulk(out, store.getex_ms(key, expires_ms));
}

pub fn mset(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, items @ ..] if !items.is_empty() && items.len() % 2 == 0 => {
            for chunk in items.chunks(2) {
                store.set(
                    chunk[0].to_string(),
                    StoreValue::string(chunk[1].to_string()),
                );
            }
            resp::write_ok(out);
        }
        _ => resp::write_wrong_args(out, "mset"),
    }
}

pub fn msetnx(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, items @ ..] if !items.is_empty() && items.len() % 2 == 0 => {
            let mut inserted: Vec<&str> = Vec::with_capacity(items.len() / 2);
            let mut all_ok = true;

            for chunk in items.chunks(2) {
                let key = chunk[0];
                let val = chunk[1];
                if store
                    .data
                    .insert_if_absent(key.to_string(), StoreValue::string(val.to_string()))
                {
                    inserted.push(key);
                } else {
                    all_ok = false;
                    break;
                }
            }

            if !all_ok {
                for key in inserted {
                    store.data.remove(key);
                }
                out.extend_from_slice(resp::ZERO);
            } else {
                out.extend_from_slice(resp::ONE);
            }
        }
        _ => resp::write_wrong_args(out, "msetnx"),
    }
}

pub fn mget(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, keys @ ..] if !keys.is_empty() => {
            resp::write_array_header(out, keys.len());
            for &k in keys.iter() {
                if !store.get_to_buf(k, out) {
                    resp::write_nil(out);
                }
            }
        }
        _ => resp::write_wrong_args(out, "mget"),
    }
}

pub fn incr(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key] => resp::write_integer(out, store_ok!(out, store.incr(key))),
        _ => resp::write_wrong_args(out, "incr"),
    }
}

pub fn decr(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key] => resp::write_integer(out, store_ok!(out, store.decr(key))),
        _ => resp::write_wrong_args(out, "decr"),
    }
}

pub fn incrby(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, by] = parts else {
        return resp::write_wrong_args(out, "incrby");
    };
    let n = parse_int!(out, by);
    resp::write_integer(out, store_ok!(out, store.incrby(key, n)));
}

pub fn decrby(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, by] = parts else {
        return resp::write_wrong_args(out, "decrby");
    };
    let n = parse_int!(out, by);
    resp::write_integer(out, store_ok!(out, store.decrby(key, n)));
}

pub fn incrbyfloat(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, by] = parts else {
        return resp::write_wrong_args(out, "incrbyfloat");
    };
    let n = parse_float!(out, by);
    resp::write_bulk(
        out,
        &format_float(store_ok!(out, store.incrbyfloat(key, n))),
    );
}

pub fn append(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key, value] => resp::write_integer(out, wt!(out, store.append(key, value)) as i64),
        _ => resp::write_wrong_args(out, "append"),
    }
}

pub fn strlen(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key] => resp::write_integer(out, wt!(out, store.strlen(key)) as i64),
        _ => resp::write_wrong_args(out, "strlen"),
    }
}

pub fn getrange(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, start, end] = parts else {
        return resp::write_wrong_args(out, "getrange");
    };
    let s = parse_int!(out, start);
    let e = parse_int!(out, end);
    resp::write_bulk(out, &store.getrange(key, s, e));
}

pub fn setrange(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, offset, value] = parts else {
        return resp::write_wrong_args(out, "setrange");
    };
    let o = parse_int!(out, offset);
    if o < 0 {
        return resp::write_err(out, "offset is out of range");
    }
    match store.setrange(key, o as usize, value) {
        Ok(n) => resp::write_integer(out, n as i64),
        Err(e) => resp::write_store_err(out, e),
    }
}

pub fn substr(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    getrange(parts, store, out);
}

pub fn lcs(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key1, key2, rest @ ..] = parts else {
        return resp::write_wrong_args(out, "lcs");
    };
    let len_only = rest.iter().any(|r| r.eq_ignore_ascii_case("LEN"));
    if len_only {
        match store.lcs_len(key1, key2) {
            Ok(n) => resp::write_integer(out, n as i64),
            Err(e) => resp::write_store_err(out, e),
        }
    } else {
        match store.lcs(key1, key2) {
            Ok(s) => resp::write_bulk(out, &s),
            Err(e) => resp::write_store_err(out, e),
        }
    }
}
