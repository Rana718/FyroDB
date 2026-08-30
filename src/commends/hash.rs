use crate::storage::store::Store;
use crate::utils::resp;
use crate::utils::util::format_float;
use crate::{parse_float, parse_int, store_ok, wt};

pub fn hset(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key, pairs @ ..] if !pairs.is_empty() && pairs.len() % 2 == 0 => {
            let fields: Vec<(&str, &str)> = pairs.chunks(2).map(|c| (c[0], c[1])).collect();
            resp::write_integer(out, wt!(out, store.hset(key, &fields)) as i64);
        }
        _ => resp::write_wrong_args(out, "hset"),
    }
}

pub fn hsetnx(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key, field, value] => {
            resp::write_boolean(out, wt!(out, store.hsetnx(key, field, value.to_string())))
        }
        _ => resp::write_wrong_args(out, "hsetnx"),
    }
}

pub fn hget(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key, field] => match wt!(out, store.hget_to_buf(key, field, out)) {
            true => {}
            false => resp::write_nil(out),
        },
        _ => resp::write_wrong_args(out, "hget"),
    }
}

pub fn hmget(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key, fields @ ..] if !fields.is_empty() => {
            resp::write_opt_array(out, &wt!(out, store.hmget(key, fields)))
        }
        _ => resp::write_wrong_args(out, "hmget"),
    }
}

pub fn hmset(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key, pairs @ ..] if !pairs.is_empty() && pairs.len() % 2 == 0 => {
            let fields: Vec<(&str, &str)> = pairs.chunks(2).map(|c| (c[0], c[1])).collect();
            wt!(out, store.hset(key, &fields));
            resp::write_ok(out);
        }
        _ => resp::write_wrong_args(out, "hmset"),
    }
}

pub fn hgetall(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key] = parts else {
        return resp::write_wrong_args(out, "hgetall");
    };
    wt!(out, store.hgetall_to_buf(key, out));
}

pub fn hdel(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key, fields @ ..] if !fields.is_empty() => {
            resp::write_integer(out, wt!(out, store.hdel(key, fields)) as i64)
        }
        _ => resp::write_wrong_args(out, "hdel"),
    }
}

pub fn hexists(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key, field] => resp::write_boolean(out, wt!(out, store.hexists(key, field))),
        _ => resp::write_wrong_args(out, "hexists"),
    }
}

pub fn hlen(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key] => resp::write_integer(out, wt!(out, store.hlen(key)) as i64),
        _ => resp::write_wrong_args(out, "hlen"),
    }
}

pub fn hkeys(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key] => resp::write_array(out, &wt!(out, store.hkeys(key))),
        _ => resp::write_wrong_args(out, "hkeys"),
    }
}

pub fn hvals(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key] => resp::write_array(out, &wt!(out, store.hvals(key))),
        _ => resp::write_wrong_args(out, "hvals"),
    }
}

pub fn hincrby(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, field, by] = parts else {
        return resp::write_wrong_args(out, "hincrby");
    };
    let n = parse_int!(out, by);
    resp::write_integer(out, store_ok!(out, store.hincrby(key, field, n)));
}

pub fn hincrbyfloat(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, field, by] = parts else {
        return resp::write_wrong_args(out, "hincrbyfloat");
    };
    let n = parse_float!(out, by);
    resp::write_bulk(
        out,
        &format_float(store_ok!(out, store.hincrbyfloat(key, field, n))),
    );
}

pub fn hrandfield(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let (key, count, withvalues) = match parts {
        [_, key] => (*key, 1i64, false),
        [_, key, cnt] => {
            let c = parse_int!(out, cnt);
            (*key, c, false)
        }
        [_, key, cnt, wv] if wv.eq_ignore_ascii_case("WITHVALUES") => {
            let c = parse_int!(out, cnt);
            (*key, c, true)
        }
        _ => return resp::write_wrong_args(out, "hrandfield"),
    };

    match store.data.get_ref(key) {
        None => {
            if parts.len() == 2 {
                resp::write_nil(out);
            } else {
                resp::write_array_header(out, 0);
            }
        }
        Some(e) if e.is_expired() => {
            if parts.len() == 2 {
                resp::write_nil(out);
            } else {
                resp::write_array_header(out, 0);
            }
        }
        Some(e) => match e.value.as_hash() {
            Some(h) => {
                if h.is_empty() {
                    if parts.len() == 2 {
                        resp::write_nil(out);
                    } else {
                        resp::write_array_header(out, 0);
                    }
                    return;
                }
                let fields: Vec<(
                    &crate::storage::value::SmallStr,
                    &crate::storage::value::SmallStr,
                )> = h.iter().collect();
                if parts.len() == 2 {
                    resp::write_bulk(out, fields[0].0);
                    return;
                }
                if count >= 0 {
                    let n = (count as usize).min(fields.len());
                    if withvalues {
                        resp::write_array_header(out, n * 2);
                        for (f, v) in fields.iter().take(n) {
                            resp::write_bulk(out, f);
                            resp::write_bulk(out, v);
                        }
                    } else {
                        resp::write_array_header(out, n);
                        for (f, _) in fields.iter().take(n) {
                            resp::write_bulk(out, f);
                        }
                    }
                } else {
                    let n = (-count) as usize;
                    let seed = crate::storage::value::now_ms();
                    if withvalues {
                        resp::write_array_header(out, n * 2);
                        for i in 0..n {
                            let idx = ((seed.wrapping_add(i as u64))
                                .wrapping_mul(0x9e3779b97f4a7c15))
                                as usize
                                % fields.len();
                            resp::write_bulk(out, fields[idx].0);
                            resp::write_bulk(out, fields[idx].1);
                        }
                    } else {
                        resp::write_array_header(out, n);
                        for i in 0..n {
                            let idx = ((seed.wrapping_add(i as u64))
                                .wrapping_mul(0x9e3779b97f4a7c15))
                                as usize
                                % fields.len();
                            resp::write_bulk(out, fields[idx].0);
                        }
                    }
                }
            }
            None => resp::write_wrong_type(out),
        },
    }
}

pub fn hscan(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, cursor_str, rest @ ..] = parts else {
        return resp::write_wrong_args(out, "hscan");
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

    match store.data.get_ref(key) {
        None => {
            out.extend_from_slice(b"*2\r\n");
            resp::write_bulk(out, "0");
            resp::write_array_header(out, 0);
        }
        Some(e) if e.is_expired() => {
            out.extend_from_slice(b"*2\r\n");
            resp::write_bulk(out, "0");
            resp::write_array_header(out, 0);
        }
        Some(e) => match e.value.as_hash() {
            Some(h) => {
                let pairs: Vec<(
                    &crate::storage::value::SmallStr,
                    &crate::storage::value::SmallStr,
                )> = h
                    .iter()
                    .filter(|(k, _)| pattern.is_none_or(|p| crate::utils::util::glob_match(p, k)))
                    .collect();
                out.extend_from_slice(b"*2\r\n");
                resp::write_bulk(out, "0");
                resp::write_array_header(out, pairs.len() * 2);
                for (f, v) in pairs {
                    resp::write_bulk(out, f);
                    resp::write_bulk(out, v);
                }
            }
            None => resp::write_wrong_type(out),
        },
    }
}
