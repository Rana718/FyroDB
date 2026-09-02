use crate::storage::store::Store;
use crate::storage::zset::ZAggregate;
use crate::utils::resp;
use crate::utils::util::format_float;
use crate::{parse_float, parse_int, store_ok, wt};

pub fn zadd(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, rest @ ..] = parts else {
        return resp::write_wrong_args(out, "zadd");
    };
    if rest.is_empty() {
        return resp::write_wrong_args(out, "zadd");
    }

    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    let mut ch = false;
    let mut i = 0;

    loop {
        if i >= rest.len() {
            return resp::write_wrong_args(out, "zadd");
        }
        let opt = rest[i];
        if opt.eq_ignore_ascii_case("NX") {
            nx = true;
        } else if opt.eq_ignore_ascii_case("XX") {
            xx = true;
        } else if opt.eq_ignore_ascii_case("GT") {
            gt = true;
        } else if opt.eq_ignore_ascii_case("LT") {
            lt = true;
        } else if opt.eq_ignore_ascii_case("CH") {
            ch = true;
        } else {
            break;
        }
        i += 1;
    }

    if nx && xx {
        return resp::write_err(out, "XX and NX options at the same time are not compatible");
    }

    let score_members = &rest[i..];
    if score_members.is_empty() || score_members.len() % 2 != 0 {
        return resp::write_wrong_args(out, "zadd");
    }

    let mut members = Vec::with_capacity(score_members.len() / 2);
    for chunk in score_members.chunks(2) {
        let score = parse_float!(out, chunk[0]);
        members.push((score, chunk[1]));
    }

    resp::write_integer(
        out,
        store_ok!(
            out,
            store.zadd(
                key,
                &members,
                crate::storage::zset::ZAddOptions { nx, xx, gt, lt, ch }
            )
        ) as i64,
    );
}

pub fn zrem(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key, members @ ..] if !members.is_empty() => {
            resp::write_integer(out, wt!(out, store.zrem(key, members)) as i64)
        }
        _ => resp::write_wrong_args(out, "zrem"),
    }
}

pub fn zscore(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key, member] => match wt!(out, store.zscore(key, member)) {
            Some(s) => resp::write_bulk(out, &format_float(s)),
            None => resp::write_nil(out),
        },
        _ => resp::write_wrong_args(out, "zscore"),
    }
}

pub fn zmscore(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key, members @ ..] if !members.is_empty() => {
            wt!(out, store.zmscore_to_buf(key, members, out));
        }
        _ => resp::write_wrong_args(out, "zmscore"),
    }
}

pub fn zrank(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key, member] => match wt!(out, store.zrank(key, member)) {
            Some(r) => resp::write_integer(out, r as i64),
            None => resp::write_nil(out),
        },
        _ => resp::write_wrong_args(out, "zrank"),
    }
}

pub fn zrevrank(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key, member] => match wt!(out, store.zrevrank(key, member)) {
            Some(r) => resp::write_integer(out, r as i64),
            None => resp::write_nil(out),
        },
        _ => resp::write_wrong_args(out, "zrevrank"),
    }
}

pub fn zcard(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key] => resp::write_integer(out, wt!(out, store.zcard(key)) as i64),
        _ => resp::write_wrong_args(out, "zcard"),
    }
}

pub fn zcount(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, min, max] = parts else {
        return resp::write_wrong_args(out, "zcount");
    };
    let mn = parse_score_bound(min, f64::NEG_INFINITY);
    let mx = parse_score_bound(max, f64::INFINITY);
    resp::write_integer(out, wt!(out, store.zcount(key, mn, mx)) as i64);
}

pub fn zincrby(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, incr, member] = parts else {
        return resp::write_wrong_args(out, "zincrby");
    };
    let by = parse_float!(out, incr);
    resp::write_bulk(
        out,
        &format_float(store_ok!(out, store.zincrby(key, by, member))),
    );
}

pub fn zrange(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, start, stop, rest @ ..] = parts else {
        return resp::write_wrong_args(out, "zrange");
    };
    let s = parse_int!(out, start);
    let e = parse_int!(out, stop);
    let withscores = rest.iter().any(|r| r.eq_ignore_ascii_case("WITHSCORES"));
    wt!(out, store.zrange_to_buf(key, s, e, withscores, out));
}

pub fn zrevrange(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, start, stop, rest @ ..] = parts else {
        return resp::write_wrong_args(out, "zrevrange");
    };
    let s = parse_int!(out, start);
    let e = parse_int!(out, stop);
    let withscores = rest.iter().any(|r| r.eq_ignore_ascii_case("WITHSCORES"));
    let items = wt!(out, store.zrevrange(key, s, e));
    write_zset_result(out, &items, withscores);
}

pub fn zrangebyscore(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, min, max, rest @ ..] = parts else {
        return resp::write_wrong_args(out, "zrangebyscore");
    };
    let mn = parse_score_bound(min, f64::NEG_INFINITY);
    let mx = parse_score_bound(max, f64::INFINITY);
    let mut withscores = false;
    let mut offset = 0usize;
    let mut count = 0usize;
    let mut i = 0;
    while i < rest.len() {
        if rest[i].eq_ignore_ascii_case("WITHSCORES") {
            withscores = true;
        } else if rest[i].eq_ignore_ascii_case("LIMIT") {
            if i + 2 >= rest.len() {
                return resp::write_err(out, "syntax error");
            }
            offset = parse_int!(out, rest[i + 1], usize);
            count = parse_int!(out, rest[i + 2], usize);
            i += 2;
        }
        i += 1;
    }
    let items = wt!(out, store.zrangebyscore(key, mn, mx, offset, count));
    write_zset_result(out, &items, withscores);
}

pub fn zrevrangebyscore(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, max, min, rest @ ..] = parts else {
        return resp::write_wrong_args(out, "zrevrangebyscore");
    };
    let mx = parse_score_bound(max, f64::INFINITY);
    let mn = parse_score_bound(min, f64::NEG_INFINITY);
    let mut withscores = false;
    let mut offset = 0usize;
    let mut count = 0usize;
    let mut i = 0;
    while i < rest.len() {
        if rest[i].eq_ignore_ascii_case("WITHSCORES") {
            withscores = true;
        } else if rest[i].eq_ignore_ascii_case("LIMIT") {
            if i + 2 >= rest.len() {
                return resp::write_err(out, "syntax error");
            }
            offset = parse_int!(out, rest[i + 1], usize);
            count = parse_int!(out, rest[i + 2], usize);
            i += 2;
        }
        i += 1;
    }
    let items = wt!(out, store.zrevrangebyscore(key, mx, mn, offset, count));
    write_zset_result(out, &items, withscores);
}

pub fn zpopmin(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let (key, count) = match parts {
        [_, key] => (*key, 1usize),
        [_, key, cnt] => {
            let c = parse_int!(out, cnt, usize);
            (*key, c)
        }
        _ => return resp::write_wrong_args(out, "zpopmin"),
    };
    let items = wt!(out, store.zpopmin(key, count));
    write_zset_result(out, &items, true);
}

pub fn zpopmax(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let (key, count) = match parts {
        [_, key] => (*key, 1usize),
        [_, key, cnt] => {
            let c = parse_int!(out, cnt, usize);
            (*key, c)
        }
        _ => return resp::write_wrong_args(out, "zpopmax"),
    };
    let items = wt!(out, store.zpopmax(key, count));
    write_zset_result(out, &items, true);
}

pub fn zunionstore(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, dst, numkeys_str, rest @ ..] = parts else {
        return resp::write_wrong_args(out, "zunionstore");
    };
    let numkeys = parse_int!(out, numkeys_str, usize);
    let (keys, weights, aggregate) = parse_store_args(rest, numkeys, out);
    if keys.is_empty() {
        return;
    }
    resp::write_integer(
        out,
        store_ok!(out, store.zunionstore(dst, &keys, &weights, aggregate)) as i64,
    );
}

pub fn zinterstore(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, dst, numkeys_str, rest @ ..] = parts else {
        return resp::write_wrong_args(out, "zinterstore");
    };
    let numkeys = parse_int!(out, numkeys_str, usize);
    let (keys, weights, aggregate) = parse_store_args(rest, numkeys, out);
    if keys.is_empty() {
        return;
    }
    resp::write_integer(
        out,
        store_ok!(out, store.zinterstore(dst, &keys, &weights, aggregate)) as i64,
    );
}

pub fn zrandmember(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let (key, count, withscores) = match parts {
        [_, key] => (*key, 1i64, false),
        [_, key, cnt] => {
            let c = parse_int!(out, cnt);
            (*key, c, false)
        }
        [_, key, cnt, ws] if ws.eq_ignore_ascii_case("WITHSCORES") => {
            let c = parse_int!(out, cnt);
            (*key, c, true)
        }
        _ => return resp::write_wrong_args(out, "zrandmember"),
    };
    let items = wt!(out, store.zrandmember(key, count));
    if parts.len() == 2 {
        if items.is_empty() {
            resp::write_nil(out);
        } else {
            resp::write_bulk(out, &items[0].0);
        }
    } else {
        write_zset_result(out, &items, withscores);
    }
}

fn write_zset_result(out: &mut Vec<u8>, items: &[(String, f64)], withscores: bool) {
    if withscores {
        resp::write_array_header(out, items.len() * 2);
        for (member, score) in items {
            resp::write_bulk(out, member);
            resp::write_bulk(out, &format_float(*score));
        }
    } else {
        resp::write_array_header(out, items.len());
        for (member, _) in items {
            resp::write_bulk(out, member);
        }
    }
}

fn parse_score_bound(s: &str, default: f64) -> f64 {
    if s == "-inf" || s == "-INF" {
        f64::NEG_INFINITY
    } else if s == "+inf" || s == "+INF" || s == "inf" || s == "INF" {
        f64::INFINITY
    } else if let Some(stripped) = s.strip_prefix('(') {
        stripped.parse::<f64>().unwrap_or(default) + f64::EPSILON
    } else {
        s.parse::<f64>().unwrap_or(default)
    }
}

fn parse_store_args<'a>(
    rest: &'a [&'a str],
    numkeys: usize,
    out: &mut Vec<u8>,
) -> (Vec<&'a str>, Vec<f64>, ZAggregate) {
    if rest.len() < numkeys {
        resp::write_wrong_args(out, "z*store");
        return (vec![], vec![], ZAggregate::Sum);
    }
    let keys: Vec<&str> = rest[..numkeys].to_vec();
    let mut weights = Vec::new();
    let mut aggregate = ZAggregate::Sum;
    let mut i = numkeys;
    while i < rest.len() {
        if rest[i].eq_ignore_ascii_case("WEIGHTS") {
            i += 1;
            for _ in 0..numkeys {
                if i >= rest.len() {
                    resp::write_err(out, "syntax error");
                    return (vec![], vec![], ZAggregate::Sum);
                }
                match rest[i].parse::<f64>() {
                    Ok(w) => weights.push(w),
                    Err(_) => {
                        resp::write_err(out, "weight is not a float");
                        return (vec![], vec![], ZAggregate::Sum);
                    }
                }
                i += 1;
            }
        } else if rest[i].eq_ignore_ascii_case("AGGREGATE") {
            i += 1;
            if i >= rest.len() {
                resp::write_err(out, "syntax error");
                return (vec![], vec![], ZAggregate::Sum);
            }
            aggregate = match ZAggregate::parse_str(rest[i]) {
                Some(a) => a,
                None => {
                    resp::write_err(out, "syntax error");
                    return (vec![], vec![], ZAggregate::Sum);
                }
            };
            i += 1;
        } else {
            resp::write_err(out, "syntax error");
            return (vec![], vec![], ZAggregate::Sum);
        }
    }
    (keys, weights, aggregate)
}

pub fn zlexcount(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, min, max] = parts else {
        return resp::write_wrong_args(out, "zlexcount");
    };
    resp::write_integer(out, wt!(out, store.zlexcount(key, min, max)) as i64);
}

pub fn zrangebylex(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, min, max, rest @ ..] = parts else {
        return resp::write_wrong_args(out, "zrangebylex");
    };
    let mut offset = 0usize;
    let mut count = 0usize;
    let mut i = 0;
    while i < rest.len() {
        if rest[i].eq_ignore_ascii_case("LIMIT") {
            if i + 2 >= rest.len() {
                return resp::write_err(out, "syntax error");
            }
            offset = parse_int!(out, rest[i + 1], usize);
            count = parse_int!(out, rest[i + 2], usize);
            i += 2;
        } else {
            return resp::write_err(out, "syntax error");
        }
        i += 1;
    }
    let items = wt!(out, store.zrangebylex(key, min, max, offset, count));
    resp::write_array(out, &items);
}

pub fn zdiff(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, numkeys_str, rest @ ..] = parts else {
        return resp::write_wrong_args(out, "zdiff");
    };
    let numkeys = parse_int!(out, numkeys_str, usize);
    if rest.len() < numkeys {
        return resp::write_wrong_args(out, "zdiff");
    }
    let keys = &rest[..numkeys];
    let withscores = rest[numkeys..]
        .iter()
        .any(|r| r.eq_ignore_ascii_case("WITHSCORES"));
    let items = wt!(out, store.zdiff(keys));
    write_zset_result(out, &items, withscores);
}

pub fn zdiffstore(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, dst, numkeys_str, rest @ ..] = parts else {
        return resp::write_wrong_args(out, "zdiffstore");
    };
    let numkeys = parse_int!(out, numkeys_str, usize);
    if rest.len() < numkeys {
        return resp::write_wrong_args(out, "zdiffstore");
    }
    let keys = &rest[..numkeys];
    resp::write_integer(out, store_ok!(out, store.zdiffstore(dst, keys)) as i64);
}

pub fn zunion(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, numkeys_str, rest @ ..] = parts else {
        return resp::write_wrong_args(out, "zunion");
    };
    let numkeys = parse_int!(out, numkeys_str, usize);
    let (keys, weights, aggregate) = parse_store_args(rest, numkeys, out);
    if keys.is_empty() {
        return;
    }
    let remaining = &rest[numkeys..];
    let withscores = remaining
        .iter()
        .any(|r| r.eq_ignore_ascii_case("WITHSCORES"));
    let items = wt!(out, store.zunion(&keys, &weights, aggregate));
    write_zset_result(out, &items, withscores);
}

pub fn zinter(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, numkeys_str, rest @ ..] = parts else {
        return resp::write_wrong_args(out, "zinter");
    };
    let numkeys = parse_int!(out, numkeys_str, usize);
    let (keys, weights, aggregate) = parse_store_args(rest, numkeys, out);
    if keys.is_empty() {
        return;
    }
    let remaining = &rest[numkeys..];
    let withscores = remaining
        .iter()
        .any(|r| r.eq_ignore_ascii_case("WITHSCORES"));
    let items = wt!(out, store.zinter(&keys, &weights, aggregate));
    write_zset_result(out, &items, withscores);
}

pub fn zscan(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, cursor_str, rest @ ..] = parts else {
        return resp::write_wrong_args(out, "zscan");
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
        Some(e) => match e.value.as_zset() {
            Some(z) => {
                let pairs: Vec<(&crate::storage::value::SmallStr, f64)> = z
                    .iter()
                    .map(|e| (&e.member, e.score))
                    .filter(|(m, _)| {
                        pattern.is_none_or(|p| crate::utils::util::glob_match(p, m.as_str()))
                    })
                    .collect();
                out.extend_from_slice(b"*2\r\n");
                resp::write_bulk(out, "0");
                resp::write_array_header(out, pairs.len() * 2);
                for (m, s) in pairs {
                    resp::write_bulk(out, m);
                    resp::write_bulk(out, &format_float(s));
                }
            }
            None => resp::write_wrong_type(out),
        },
    }
}

pub fn zrangestore(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, dst, src, min, max, _rest @ ..] = parts else {
        return resp::write_wrong_args(out, "zrangestore");
    };
    let s = parse_int!(out, min);
    let e = parse_int!(out, max);
    let items = wt!(out, store.zrange(src, s, e, false));
    let mut z = crate::storage::value::ZSetData::new();
    for (member, score) in &items {
        z.insert(*score, member.as_str());
    }
    let count = z.len();
    store
        .data
        .insert(dst.to_string(), crate::storage::value::StoreValue::zset(z));
    resp::write_integer(out, count as i64);
}

pub fn bzpopmin(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    if parts.len() < 3 {
        return resp::write_wrong_args(out, "bzpopmin");
    }
    let keys = &parts[1..parts.len() - 1];
    for &k in keys {
        let items = match store.zpopmin(k, 1) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if !items.is_empty() {
            resp::write_array_header(out, 3);
            resp::write_bulk(out, k);
            resp::write_bulk(out, &items[0].0);
            resp::write_bulk(out, &format_float(items[0].1));
            return;
        }
    }
    resp::write_nil(out);
}

pub fn bzpopmax(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    if parts.len() < 3 {
        return resp::write_wrong_args(out, "bzpopmax");
    }
    let keys = &parts[1..parts.len() - 1];
    for &k in keys {
        let items = match store.zpopmax(k, 1) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if !items.is_empty() {
            resp::write_array_header(out, 3);
            resp::write_bulk(out, k);
            resp::write_bulk(out, &items[0].0);
            resp::write_bulk(out, &format_float(items[0].1));
            return;
        }
    }
    resp::write_nil(out);
}
