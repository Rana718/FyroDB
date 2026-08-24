use std::sync::Arc;

use crate::commends;
use crate::pubsub::encode_sub_reply;
use crate::utils::resp;

use super::conn::{Conn, ConnMode};
use super::pubsub_cmds::pubsub_info;
use super::subscription::{
    handle_psubscribe, handle_punsubscribe, handle_subscribe, handle_unsubscribe,
};

pub fn dispatch(conn: &mut Conn, parts: &[&str]) {
    if parts.is_empty() {
        conn.parser
            .wbuf
            .extend_from_slice(b"-ERR empty command\r\n");
        return;
    }

    let cmd = parts[0].as_bytes();

    match &conn.mode {
        ConnMode::Normal => {
            match cmd.first().map(|b| b.to_ascii_uppercase()) {
                Some(b'S') => {
                    if cmd_eq(cmd, b"SET") {
                        let response_start = conn.parser.wbuf.len();
                        commends::string::set(parts, &conn.store, &mut conn.parser.wbuf);
                        capture_if_success(conn, parts, response_start);
                        return;
                    } else if cmd_eq(cmd, b"SADD") {
                        let response_start = conn.parser.wbuf.len();
                        commends::set::sadd(parts, &conn.store, &mut conn.parser.wbuf);
                        capture_if_success(conn, parts, response_start);
                        return;
                    }
                }
                Some(b'G') if cmd_eq(cmd, b"GET") => {
                    return commends::string::get(parts, &conn.store, &mut conn.parser.wbuf);
                }
                Some(b'D') if cmd_eq(cmd, b"DEL") => {
                    return commends::keys::del(parts, &conn.store, &mut conn.parser.wbuf);
                }
                Some(b'I') if cmd_eq(cmd, b"INCR") => {
                    let response_start = conn.parser.wbuf.len();
                    commends::string::incr(parts, &conn.store, &mut conn.parser.wbuf);
                    capture_if_success(conn, parts, response_start);
                    return;
                }
                Some(b'H') => {
                    if cmd_eq(cmd, b"HSET") {
                        let response_start = conn.parser.wbuf.len();
                        commends::hash::hset(parts, &conn.store, &mut conn.parser.wbuf);
                        capture_if_success(conn, parts, response_start);
                        return;
                    } else if cmd_eq(cmd, b"HGET") {
                        return commends::hash::hget(parts, &conn.store, &mut conn.parser.wbuf);
                    }
                }
                Some(b'L') => {
                    if cmd_eq(cmd, b"LPUSH") {
                        let response_start = conn.parser.wbuf.len();
                        commends::list::lpush(parts, &conn.store, &mut conn.parser.wbuf);
                        capture_if_success(conn, parts, response_start);
                        return;
                    } else if cmd_eq(cmd, b"LPOP") {
                        let response_start = conn.parser.wbuf.len();
                        commends::list::lpop(parts, &conn.store, &mut conn.parser.wbuf);
                        capture_if_success(conn, parts, response_start);
                        return;
                    } else if cmd_eq(cmd, b"LRANGE") {
                        return commends::list::lrange(parts, &conn.store, &mut conn.parser.wbuf);
                    }
                }
                Some(b'R') => {
                    if cmd_eq(cmd, b"RPUSH") {
                        let response_start = conn.parser.wbuf.len();
                        commends::list::rpush(parts, &conn.store, &mut conn.parser.wbuf);
                        capture_if_success(conn, parts, response_start);
                        return;
                    } else if cmd_eq(cmd, b"RPOP") {
                        let response_start = conn.parser.wbuf.len();
                        commends::list::rpop(parts, &conn.store, &mut conn.parser.wbuf);
                        capture_if_success(conn, parts, response_start);
                        return;
                    }
                }
                Some(b'E') if cmd_eq(cmd, b"EXPIRE") => {
                    return commends::keys::expire(parts, &conn.store, &mut conn.parser.wbuf);
                }
                Some(b'Z') if cmd_eq(cmd, b"ZADD") => {
                    let response_start = conn.parser.wbuf.len();
                    commends::zset::zadd(parts, &conn.store, &mut conn.parser.wbuf);
                    capture_if_success(conn, parts, response_start);
                    return;
                }
                Some(b'J') if cmd.len() == 8 => {
                    if cmd_eq(cmd, b"JSON.SET") {
                        let response_start = conn.parser.wbuf.len();
                        commends::json::json_set(parts, &conn.store, &mut conn.parser.wbuf);
                        capture_if_success(conn, parts, response_start);
                        return;
                    } else if cmd_eq(cmd, b"JSON.GET") {
                        return commends::json::json_get(parts, &conn.store, &mut conn.parser.wbuf);
                    }
                }
                _ => {}
            }

            if cmd_eq(cmd, b"SUBSCRIBE") {
                handle_subscribe(conn, parts);
            } else if cmd_eq(cmd, b"PSUBSCRIBE") {
                handle_psubscribe(conn, parts);
            } else if cmd_eq(cmd, b"UNSUBSCRIBE") || cmd_eq(cmd, b"PUNSUBSCRIBE") {
                conn.parser
                    .wbuf
                    .extend_from_slice(&encode_sub_reply("unsubscribe", "", 0));
            } else if cmd_eq(cmd, b"PUBLISH") {
                match parts {
                    [_, channel, message] => {
                        let n = conn.pubsub.publish(channel, message);
                        resp::write_integer(&mut conn.parser.wbuf, n as i64);
                    }
                    _ => resp::write_wrong_args(&mut conn.parser.wbuf, "publish"),
                }
            } else if cmd_eq(cmd, b"PUBSUB") {
                let pubsub = Arc::clone(&conn.pubsub);
                pubsub_info(parts, &pubsub, &mut conn.parser.wbuf);
            } else {
                let response_start = conn.parser.wbuf.len();
                commends::execute(parts, &conn.store, &mut conn.parser.wbuf);
                capture_if_success(conn, parts, response_start);
            }
        }

        ConnMode::Subscribed { .. } => {
            if cmd_eq(cmd, b"SUBSCRIBE") {
                handle_subscribe(conn, parts);
            } else if cmd_eq(cmd, b"UNSUBSCRIBE") {
                handle_unsubscribe(conn, parts);
            } else if cmd_eq(cmd, b"PSUBSCRIBE") {
                handle_psubscribe(conn, parts);
            } else if cmd_eq(cmd, b"PUNSUBSCRIBE") {
                handle_punsubscribe(conn, parts);
            } else if cmd_eq(cmd, b"ASKING") {
                // Redis Cluster clients send ASKING immediately before a
                // redirected command. FyroDB's migration fence is enforced
                // by the routing layer, so the marker itself is a no-op.
                conn.asking = true;
                resp::write_simple(&mut conn.parser.wbuf, "OK");
            } else if cmd_eq(cmd, b"PING") {
                let out = &mut conn.parser.wbuf;
                let msg = parts.get(1).copied().unwrap_or("");
                if msg.is_empty() {
                    out.extend_from_slice(b"*2\r\n$4\r\npong\r\n$0\r\n\r\n");
                } else {
                    out.extend_from_slice(b"*2\r\n$4\r\npong\r\n");
                    resp::write_bulk(out, msg);
                }
            } else if cmd_eq(cmd, b"RESET") || cmd_eq(cmd, b"QUIT") {
                super::subscription::do_full_unsubscribe(conn);
                resp::write_simple(&mut conn.parser.wbuf, "OK");
            } else {
                conn.parser
                    .wbuf
                    .extend_from_slice(b"-ERR Command not allowed in subscribed state\r\n");
            }
        }
    }
}

#[inline]
fn capture_if_success(conn: &mut Conn, parts: &[&str], response_start: usize) {
    let response = &conn.parser.wbuf[response_start..];
    if !response.starts_with(b"-") && response != b"$-1\r\n" {
        capture_command_mutation(conn, parts);
    }
}

fn capture_command_mutation(conn: &mut Conn, parts: &[&str]) {
    let Some(command) = parts.first() else { return };
    let command = command.as_bytes();
    let capture = |conn: &Conn, index: usize| {
        if let Some(key) = parts.get(index) {
            conn.store.record_current_value(key);
        }
    };

    if command.eq_ignore_ascii_case(b"MSET") || command.eq_ignore_ascii_case(b"MSETNX") {
        for index in (1..parts.len()).step_by(2) {
            capture(conn, index);
        }
        return;
    }
    if command.eq_ignore_ascii_case(b"RENAME")
        || command.eq_ignore_ascii_case(b"RENAMENX")
        || command.eq_ignore_ascii_case(b"SMOVE")
        || command.eq_ignore_ascii_case(b"LMOVE")
        || command.eq_ignore_ascii_case(b"RPOPLPUSH")
    {
        capture(conn, 1);
        capture(conn, 2);
        return;
    }
    if command.eq_ignore_ascii_case(b"COPY") {
        capture(conn, 2);
        return;
    }
    if command.eq_ignore_ascii_case(b"BITOP") {
        capture(conn, 2);
        return;
    }
    if command.eq_ignore_ascii_case(b"SUNIONSTORE")
        || command.eq_ignore_ascii_case(b"SINTERSTORE")
        || command.eq_ignore_ascii_case(b"SDIFFSTORE")
        || command.eq_ignore_ascii_case(b"ZUNIONSTORE")
        || command.eq_ignore_ascii_case(b"ZINTERSTORE")
        || command.eq_ignore_ascii_case(b"ZDIFFSTORE")
        || command.eq_ignore_ascii_case(b"PFMERGE")
        || command.eq_ignore_ascii_case(b"GEOSEARCHSTORE")
    {
        capture(conn, 1);
        return;
    }

    const FIRST_KEY_MUTATIONS: &[&[u8]] = &[
        b"SET",
        b"SETNX",
        b"SETEX",
        b"PSETEX",
        b"GETDEL",
        b"GETSET",
        b"GETEX",
        b"INCR",
        b"DECR",
        b"INCRBY",
        b"DECRBY",
        b"INCRBYFLOAT",
        b"APPEND",
        b"SETRANGE",
        b"PERSIST",
        b"HSET",
        b"HSETNX",
        b"HMSET",
        b"HDEL",
        b"HINCRBY",
        b"HINCRBYFLOAT",
        b"LPUSH",
        b"RPUSH",
        b"LPOP",
        b"RPOP",
        b"LSET",
        b"LTRIM",
        b"LREM",
        b"LINSERT",
        b"SADD",
        b"SREM",
        b"SPOP",
        b"ZADD",
        b"ZREM",
        b"ZINCRBY",
        b"ZPOPMIN",
        b"ZPOPMAX",
        b"SETBIT",
        b"PFADD",
        b"JSON.SET",
        b"JSON.DEL",
        b"JSON.NUMINCRBY",
        b"JSON.NUMMULTBY",
        b"JSON.STRAPPEND",
        b"JSON.ARRAPPEND",
        b"JSON.ARRINSERT",
        b"JSON.ARRPOP",
        b"JSON.ARRTRIM",
        b"JSON.TOGGLE",
        b"JSON.CLEAR",
        b"XADD",
        b"XTRIM",
        b"XDEL",
        b"XGROUP",
        b"XACK",
        b"GEOADD",
    ];
    if FIRST_KEY_MUTATIONS
        .iter()
        .any(|known| command.eq_ignore_ascii_case(known))
    {
        capture(conn, 1);
    }
}

#[inline(always)]
pub fn cmd_eq(a: &[u8], upper: &[u8]) -> bool {
    a.len() == upper.len()
        && a.iter()
            .zip(upper.iter())
            .all(|(&ac, &uc)| ac.to_ascii_uppercase() == uc)
}
