use crate::storage::store::Store;
use crate::utils::resp;
use crate::{parse_int, wt};

pub fn lpush(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key, values @ ..] if !values.is_empty() => {
            resp::write_integer(out, wt!(out, store.lpush(key, values)) as i64)
        }
        _ => resp::write_wrong_args(out, "lpush"),
    }
}

pub fn rpush(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key, values @ ..] if !values.is_empty() => {
            resp::write_integer(out, wt!(out, store.rpush(key, values)) as i64)
        }
        _ => resp::write_wrong_args(out, "rpush"),
    }
}

pub fn lpop(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    if let [_, key] = parts {
        return match wt!(out, store.pop_one_to_buf(key, false, out)) {
            true => {}
            false => resp::write_nil(out),
        };
    }
    let (key, count) = match parts {
        [_, key, cnt] => {
            let c = parse_int!(out, cnt, usize);
            (*key, c)
        }
        _ => return resp::write_wrong_args(out, "lpop"),
    };
    let items = wt!(out, store.lpop(key, count));
    if items.is_empty() && parts.len() == 2 {
        resp::write_nil(out);
    } else {
        resp::write_array(out, &items);
    }
}

pub fn rpop(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    if let [_, key] = parts {
        return match wt!(out, store.pop_one_to_buf(key, true, out)) {
            true => {}
            false => resp::write_nil(out),
        };
    }
    let (key, count) = match parts {
        [_, key, cnt] => {
            let c = parse_int!(out, cnt, usize);
            (*key, c)
        }
        _ => return resp::write_wrong_args(out, "rpop"),
    };
    let items = wt!(out, store.rpop(key, count));
    if items.is_empty() && parts.len() == 2 {
        resp::write_nil(out);
    } else {
        resp::write_array(out, &items);
    }
}

pub fn llen(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key] => resp::write_integer(out, wt!(out, store.llen(key)) as i64),
        _ => resp::write_wrong_args(out, "llen"),
    }
}

pub fn lindex(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, idx] = parts else {
        return resp::write_wrong_args(out, "lindex");
    };
    let i = parse_int!(out, idx);
    resp::write_opt_bulk(out, wt!(out, store.lindex(key, i)));
}

pub fn lset(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, idx, value] = parts else {
        return resp::write_wrong_args(out, "lset");
    };
    let i = parse_int!(out, idx);
    match store.lset(key, i, value) {
        Ok(_) => resp::write_ok(out),
        Err(e) => resp::write_store_err(out, e),
    }
}

pub fn lrange(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, start, stop] = parts else {
        return resp::write_wrong_args(out, "lrange");
    };
    let s = parse_int!(out, start);
    let e = parse_int!(out, stop);
    let items = wt!(out, store.lrange(key, s, e));
    resp::write_array(out, &items);
}

pub fn ltrim(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, start, stop] = parts else {
        return resp::write_wrong_args(out, "ltrim");
    };
    let s = parse_int!(out, start);
    let e = parse_int!(out, stop);
    wt!(out, store.ltrim(key, s, e));
    resp::write_ok(out);
}

pub fn lrem(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, count, value] = parts else {
        return resp::write_wrong_args(out, "lrem");
    };
    let c = parse_int!(out, count);
    resp::write_integer(out, wt!(out, store.lrem(key, c, value)) as i64);
}

pub fn linsert(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, pos, pivot, value] = parts else {
        return resp::write_wrong_args(out, "linsert");
    };
    let before = if pos.eq_ignore_ascii_case("BEFORE") {
        true
    } else if pos.eq_ignore_ascii_case("AFTER") {
        false
    } else {
        return resp::write_err(out, "syntax error");
    };
    resp::write_integer(out, wt!(out, store.linsert(key, before, pivot, value)));
}

pub fn lpos(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, element, rest @ ..] = parts else {
        return resp::write_wrong_args(out, "lpos");
    };
    let mut rank = 1i64;
    let mut count = 1usize;
    let mut maxlen = 0usize;
    let mut count_specified = false;
    let mut i = 0;
    while i < rest.len() {
        if rest[i].eq_ignore_ascii_case("RANK") {
            i += 1;
            if i >= rest.len() {
                return resp::write_err(out, "syntax error");
            }
            rank = parse_int!(out, rest[i]);
            if rank == 0 {
                return resp::write_err(out, "RANK can't be zero");
            }
        } else if rest[i].eq_ignore_ascii_case("COUNT") {
            i += 1;
            if i >= rest.len() {
                return resp::write_err(out, "syntax error");
            }
            count = parse_int!(out, rest[i], usize);
            count_specified = true;
        } else if rest[i].eq_ignore_ascii_case("MAXLEN") {
            i += 1;
            if i >= rest.len() {
                return resp::write_err(out, "syntax error");
            }
            maxlen = parse_int!(out, rest[i], usize);
        } else {
            return resp::write_err(out, "syntax error");
        }
        i += 1;
    }
    let results = wt!(out, store.lpos(key, element, rank, count, maxlen));
    if !count_specified {
        if results.is_empty() {
            resp::write_nil(out);
        } else {
            resp::write_integer(out, results[0] as i64);
        }
    } else {
        resp::write_array_header(out, results.len());
        for r in results {
            resp::write_integer(out, r as i64);
        }
    }
}

pub fn lmove(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, src, dst, src_dir, dst_dir] = parts else {
        return resp::write_wrong_args(out, "lmove");
    };
    let left_src = if src_dir.eq_ignore_ascii_case("LEFT") {
        true
    } else if src_dir.eq_ignore_ascii_case("RIGHT") {
        false
    } else {
        return resp::write_err(out, "syntax error");
    };
    let left_dst = if dst_dir.eq_ignore_ascii_case("LEFT") {
        true
    } else if dst_dir.eq_ignore_ascii_case("RIGHT") {
        false
    } else {
        return resp::write_err(out, "syntax error");
    };
    match store.lmove(src, dst, left_src, left_dst) {
        Ok(Some(v)) => resp::write_bulk(out, &v),
        Ok(None) => resp::write_nil(out),
        Err(e) => resp::write_store_err(out, e),
    }
}

pub fn rpoplpush(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, src, dst] = parts else {
        return resp::write_wrong_args(out, "rpoplpush");
    };
    match store.lmove(src, dst, false, true) {
        Ok(Some(v)) => resp::write_bulk(out, &v),
        Ok(None) => resp::write_nil(out),
        Err(e) => resp::write_store_err(out, e),
    }
}

pub fn blpop(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    if parts.len() < 3 {
        return resp::write_wrong_args(out, "blpop");
    }
    let _timeout = parts[parts.len() - 1];
    let keys = &parts[1..parts.len() - 1];
    for &k in keys {
        let items = match store.lpop(k, 1) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if !items.is_empty() {
            resp::write_array_header(out, 2);
            resp::write_bulk(out, k);
            resp::write_bulk(out, &items[0]);
            return;
        }
    }
    resp::write_nil(out);
}

pub fn brpop(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    if parts.len() < 3 {
        return resp::write_wrong_args(out, "brpop");
    }
    let _timeout = parts[parts.len() - 1];
    let keys = &parts[1..parts.len() - 1];
    for &k in keys {
        let items = match store.rpop(k, 1) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if !items.is_empty() {
            resp::write_array_header(out, 2);
            resp::write_bulk(out, k);
            resp::write_bulk(out, &items[0]);
            return;
        }
    }
    resp::write_nil(out);
}

pub fn blmove(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, src, dst, src_dir, dst_dir, _timeout] = parts else {
        return resp::write_wrong_args(out, "blmove");
    };
    let left_src = src_dir.eq_ignore_ascii_case("LEFT");
    let left_dst = dst_dir.eq_ignore_ascii_case("LEFT");
    match store.lmove(src, dst, left_src, left_dst) {
        Ok(Some(v)) => resp::write_bulk(out, &v),
        Ok(None) => resp::write_nil(out),
        Err(e) => resp::write_store_err(out, e),
    }
}
