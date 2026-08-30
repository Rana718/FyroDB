use crate::storage::store::Store;
use crate::utils::resp;
use crate::utils::util::format_float;
use crate::{parse_float, parse_int, wt};

pub fn json_set(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, path, value, rest @ ..] = parts else {
        return resp::write_wrong_args(out, "json.set");
    };
    let mut nx = false;
    let mut xx = false;
    for opt in rest.iter() {
        if opt.eq_ignore_ascii_case("NX") {
            nx = true;
        } else if opt.eq_ignore_ascii_case("XX") {
            xx = true;
        } else {
            return resp::write_err(out, "syntax error");
        }
    }
    match store.json_set(key, path, value, nx, xx) {
        Ok(true) => resp::write_ok(out),
        Ok(false) => resp::write_nil(out),
        Err(e) => resp::write_store_err(out, e),
    }
}

pub fn json_get(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, paths @ ..] = parts else {
        return resp::write_wrong_args(out, "json.get");
    };
    match wt!(out, store.json_get_to_buf(key, paths, out)) {
        true => {}
        false => resp::write_nil(out),
    }
}

pub fn json_del(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let (key, path) = match parts {
        [_, key] => (*key, "."),
        [_, key, path] => (*key, *path),
        _ => return resp::write_wrong_args(out, "json.del"),
    };
    match store.json_del(key, path) {
        Ok(n) => resp::write_integer(out, n as i64),
        Err(e) => resp::write_store_err(out, e),
    }
}

pub fn json_forget(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    json_del(parts, store, out);
}

pub fn json_type(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let (key, path) = match parts {
        [_, key] => (*key, "."),
        [_, key, path] => (*key, *path),
        _ => return resp::write_wrong_args(out, "json.type"),
    };
    match store.json_type(key, path) {
        Ok(Some(t)) => resp::write_bulk(out, t),
        Ok(None) => resp::write_nil(out),
        Err(e) => resp::write_store_err(out, e),
    }
}

pub fn json_numincrby(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, path, value] = parts else {
        return resp::write_wrong_args(out, "json.numincrby");
    };
    let by = parse_float!(out, value);
    match store.json_numincrby(key, path, by) {
        Ok(Some(n)) => resp::write_bulk(out, &format_float(n)),
        Ok(None) => resp::write_nil(out),
        Err(e) => resp::write_store_err(out, e),
    }
}

pub fn json_nummultby(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, path, value] = parts else {
        return resp::write_wrong_args(out, "json.nummultby");
    };
    let by = parse_float!(out, value);
    match store.json_nummultby(key, path, by) {
        Ok(Some(n)) => resp::write_bulk(out, &format_float(n)),
        Ok(None) => resp::write_nil(out),
        Err(e) => resp::write_store_err(out, e),
    }
}

pub fn json_strappend(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let (key, path, value) = match parts {
        [_, key, value] => (*key, ".", *value),
        [_, key, path, value] => (*key, *path, *value),
        _ => return resp::write_wrong_args(out, "json.strappend"),
    };
    let unquoted = value
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(value);
    match store.json_strappend(key, path, unquoted) {
        Ok(Some(n)) => resp::write_integer(out, n as i64),
        Ok(None) => resp::write_nil(out),
        Err(e) => resp::write_store_err(out, e),
    }
}

pub fn json_strlen(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let (key, path) = match parts {
        [_, key] => (*key, "."),
        [_, key, path] => (*key, *path),
        _ => return resp::write_wrong_args(out, "json.strlen"),
    };
    match store.json_strlen(key, path) {
        Ok(Some(n)) => resp::write_integer(out, n as i64),
        Ok(None) => resp::write_nil(out),
        Err(e) => resp::write_store_err(out, e),
    }
}

pub fn json_arrappend(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, path, values @ ..] = parts else {
        return resp::write_wrong_args(out, "json.arrappend");
    };
    if values.is_empty() {
        return resp::write_wrong_args(out, "json.arrappend");
    }
    match store.json_arrappend(key, path, values) {
        Ok(Some(n)) => resp::write_integer(out, n as i64),
        Ok(None) => resp::write_nil(out),
        Err(e) => resp::write_store_err(out, e),
    }
}

pub fn json_arrindex(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, path, value, rest @ ..] = parts else {
        return resp::write_wrong_args(out, "json.arrindex");
    };
    let start = if !rest.is_empty() {
        parse_int!(out, rest[0])
    } else {
        0i64
    };
    let stop = if rest.len() > 1 {
        parse_int!(out, rest[1])
    } else {
        0i64
    };
    match store.json_arrindex(key, path, value, start, stop) {
        Ok(n) => resp::write_integer(out, n),
        Err(e) => resp::write_store_err(out, e),
    }
}

pub fn json_arrinsert(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, path, index_str, values @ ..] = parts else {
        return resp::write_wrong_args(out, "json.arrinsert");
    };
    if values.is_empty() {
        return resp::write_wrong_args(out, "json.arrinsert");
    }
    let index = parse_int!(out, index_str);
    match store.json_arrinsert(key, path, index, values) {
        Ok(Some(n)) => resp::write_integer(out, n as i64),
        Ok(None) => resp::write_nil(out),
        Err(e) => resp::write_store_err(out, e),
    }
}

pub fn json_arrlen(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let (key, path) = match parts {
        [_, key] => (*key, "."),
        [_, key, path] => (*key, *path),
        _ => return resp::write_wrong_args(out, "json.arrlen"),
    };
    match store.json_arrlen(key, path) {
        Ok(Some(n)) => resp::write_integer(out, n as i64),
        Ok(None) => resp::write_nil(out),
        Err(e) => resp::write_store_err(out, e),
    }
}

pub fn json_arrpop(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let (key, path, index) = match parts {
        [_, key] => (*key, ".", -1i64),
        [_, key, path] => (*key, *path, -1i64),
        [_, key, path, idx] => {
            let i = parse_int!(out, idx);
            (*key, *path, i)
        }
        _ => return resp::write_wrong_args(out, "json.arrpop"),
    };
    match store.json_arrpop(key, path, index) {
        Ok(Some(s)) => resp::write_bulk(out, &s),
        Ok(None) => resp::write_nil(out),
        Err(e) => resp::write_store_err(out, e),
    }
}

pub fn json_arrtrim(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, path, start, stop] = parts else {
        return resp::write_wrong_args(out, "json.arrtrim");
    };
    let s = parse_int!(out, start);
    let e = parse_int!(out, stop);
    match store.json_arrtrim(key, path, s, e) {
        Ok(Some(n)) => resp::write_integer(out, n as i64),
        Ok(None) => resp::write_nil(out),
        Err(er) => resp::write_store_err(out, er),
    }
}

pub fn json_objkeys(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let (key, path) = match parts {
        [_, key] => (*key, "."),
        [_, key, path] => (*key, *path),
        _ => return resp::write_wrong_args(out, "json.objkeys"),
    };
    match store.json_objkeys(key, path) {
        Ok(Some(keys)) => resp::write_array(out, &keys),
        Ok(None) => resp::write_nil(out),
        Err(e) => resp::write_store_err(out, e),
    }
}

pub fn json_objlen(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let (key, path) = match parts {
        [_, key] => (*key, "."),
        [_, key, path] => (*key, *path),
        _ => return resp::write_wrong_args(out, "json.objlen"),
    };
    match store.json_objlen(key, path) {
        Ok(Some(n)) => resp::write_integer(out, n as i64),
        Ok(None) => resp::write_nil(out),
        Err(e) => resp::write_store_err(out, e),
    }
}

pub fn json_toggle(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, path] = parts else {
        return resp::write_wrong_args(out, "json.toggle");
    };
    match store.json_toggle(key, path) {
        Ok(Some(b)) => resp::write_bulk(out, if b { "true" } else { "false" }),
        Ok(None) => resp::write_nil(out),
        Err(e) => resp::write_store_err(out, e),
    }
}

pub fn json_clear(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let (key, path) = match parts {
        [_, key] => (*key, "."),
        [_, key, path] => (*key, *path),
        _ => return resp::write_wrong_args(out, "json.clear"),
    };
    match store.json_clear(key, path) {
        Ok(n) => resp::write_integer(out, n as i64),
        Err(e) => resp::write_store_err(out, e),
    }
}

pub fn json_mget(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    if parts.len() < 3 {
        return resp::write_wrong_args(out, "json.mget");
    }
    let path = parts[parts.len() - 1];
    let keys = &parts[1..parts.len() - 1];
    resp::write_array_header(out, keys.len());
    for &k in keys {
        match wt!(out, store.json_get_to_buf(k, &[path], out)) {
            true => {}
            false => resp::write_nil(out),
        }
    }
}

pub fn json_resp(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let (key, path) = match parts {
        [_, key] => (*key, "."),
        [_, key, path] => (*key, *path),
        _ => return resp::write_wrong_args(out, "json.resp"),
    };
    match wt!(out, store.json_get_to_buf(key, &[path], out)) {
        true => {}
        false => resp::write_nil(out),
    }
}
