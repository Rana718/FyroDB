use crate::commends;
use crate::pubsub::encode_sub_reply;
use crate::utils::resp;

use super::conn::{Conn, ConnMode};
use super::pubsub_cmds::pubsub_info;
use super::subscription::{
    handle_psubscribe, handle_punsubscribe, handle_subscribe, handle_unsubscribe,
};

pub fn dispatch(conn: &mut Conn<'_>, parts: &[&str]) {
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
                        commends::string::set(parts, conn.store, &mut conn.parser.wbuf);
                        capture_if_success(conn, parts, response_start);
                        return;
                    } else if cmd_eq(cmd, b"SADD") {
                        let response_start = conn.parser.wbuf.len();
                        commends::set::sadd(parts, conn.store, &mut conn.parser.wbuf);
                        capture_if_success(conn, parts, response_start);
                        return;
                    }
                }
                Some(b'G') if cmd_eq(cmd, b"GET") => {
                    return commends::string::get(parts, conn.store, &mut conn.parser.wbuf);
                }
                Some(b'D') if cmd_eq(cmd, b"DEL") => {
                    return commends::keys::del(parts, conn.store, &mut conn.parser.wbuf);
                }
                Some(b'I') if cmd_eq(cmd, b"INCR") => {
                    let response_start = conn.parser.wbuf.len();
                    commends::string::incr(parts, conn.store, &mut conn.parser.wbuf);
                    capture_if_success(conn, parts, response_start);
                    return;
                }
                Some(b'H') => {
                    if cmd_eq(cmd, b"HSET") {
                        let response_start = conn.parser.wbuf.len();
                        commends::hash::hset(parts, conn.store, &mut conn.parser.wbuf);
                        capture_if_success(conn, parts, response_start);
                        return;
                    } else if cmd_eq(cmd, b"HGET") {
                        return commends::hash::hget(parts, conn.store, &mut conn.parser.wbuf);
                    }
                }
                Some(b'L') => {
                    if cmd_eq(cmd, b"LPUSH") {
                        let response_start = conn.parser.wbuf.len();
                        commends::list::lpush(parts, conn.store, &mut conn.parser.wbuf);
                        capture_if_success(conn, parts, response_start);
                        return;
                    } else if cmd_eq(cmd, b"LPOP") {
                        let response_start = conn.parser.wbuf.len();
                        commends::list::lpop(parts, conn.store, &mut conn.parser.wbuf);
                        capture_if_success(conn, parts, response_start);
                        return;
                    } else if cmd_eq(cmd, b"LRANGE") {
                        return commends::list::lrange(parts, conn.store, &mut conn.parser.wbuf);
                    }
                }
                Some(b'R') => {
                    if cmd_eq(cmd, b"RPUSH") {
                        let response_start = conn.parser.wbuf.len();
                        commends::list::rpush(parts, conn.store, &mut conn.parser.wbuf);
                        capture_if_success(conn, parts, response_start);
                        return;
                    } else if cmd_eq(cmd, b"RPOP") {
                        let response_start = conn.parser.wbuf.len();
                        commends::list::rpop(parts, conn.store, &mut conn.parser.wbuf);
                        capture_if_success(conn, parts, response_start);
                        return;
                    }
                }
                Some(b'E') if cmd_eq(cmd, b"EXPIRE") => {
                    return commends::keys::expire(parts, conn.store, &mut conn.parser.wbuf);
                }
                Some(b'Z') if cmd_eq(cmd, b"ZADD") => {
                    let response_start = conn.parser.wbuf.len();
                    commends::zset::zadd(parts, conn.store, &mut conn.parser.wbuf);
                    capture_if_success(conn, parts, response_start);
                    return;
                }
                Some(b'J') if cmd.len() == 8 => {
                    if cmd_eq(cmd, b"JSON.SET") {
                        let response_start = conn.parser.wbuf.len();
                        commends::json::json_set(parts, conn.store, &mut conn.parser.wbuf);
                        capture_if_success(conn, parts, response_start);
                        return;
                    } else if cmd_eq(cmd, b"JSON.GET") {
                        return commends::json::json_get(parts, conn.store, &mut conn.parser.wbuf);
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
                pubsub_info(parts, conn.pubsub, &mut conn.parser.wbuf);
            } else {
                let response_start = conn.parser.wbuf.len();
                commends::execute(parts, conn.store, &mut conn.parser.wbuf);
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
                //  Cluster clients send ASKING immediately before a
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
    if !conn.store.has_replication() {
        return;
    }
    let response = &conn.parser.wbuf[response_start..];
    if !response.starts_with(b"-") && response != b"$-1\r\n" {
        capture_command_mutation(conn, parts);
    }
}

/// Returns the key index to capture for replication, or `None` if this
/// command does not need capture. Dispatches on `(len, first_byte)` for a
/// compiler-generated jump table.
#[inline]
fn mutation_key_index(command: &[u8]) -> Option<usize> {
    let first = command.first().map(|b| b.to_ascii_uppercase())?;
    match (command.len(), first) {
        (4, b'M') if command.eq_ignore_ascii_case(b"MSET") => return None,
        (7, b'M') if command.eq_ignore_ascii_case(b"MSETNX") => return None,
        (6, b'R') if command.eq_ignore_ascii_case(b"RENAME") => return None,
        (8, b'R') if command.eq_ignore_ascii_case(b"RENAMENX") => return None,
        (5, b'S') if command.eq_ignore_ascii_case(b"SMOVE") => return None,
        (5, b'L') if command.eq_ignore_ascii_case(b"LMOVE") => return None,
        (9, b'R') if command.eq_ignore_ascii_case(b"RPOPLPUSH") => return None,
        (4, b'C') if command.eq_ignore_ascii_case(b"COPY") => return None,
        (5, b'B') if command.eq_ignore_ascii_case(b"BITOP") => return None,
        (12, b'S') if command.eq_ignore_ascii_case(b"SUNIONSTORE") => return Some(1),
        (12, b'S') if command.eq_ignore_ascii_case(b"SINTERSTORE") => return Some(1),
        (10, b'S') if command.eq_ignore_ascii_case(b"SDIFFSTORE") => return Some(1),
        (12, b'Z') if command.eq_ignore_ascii_case(b"ZUNIONSTORE") => return Some(1),
        (12, b'Z') if command.eq_ignore_ascii_case(b"ZINTERSTORE") => return Some(1),
        (10, b'Z') if command.eq_ignore_ascii_case(b"ZDIFFSTORE") => return Some(1),
        (7, b'P') if command.eq_ignore_ascii_case(b"PFMERGE") => return Some(1),
        (14, b'G') if command.eq_ignore_ascii_case(b"GEOSEARCHSTORE") => return Some(1),
        _ => {}
    }
    let is_mutation = match first {
        b'S' => {
            matches!(
                command.len(),
                3 if command.eq_ignore_ascii_case(b"SET")
            ) || command.eq_ignore_ascii_case(b"SETNX")
                || command.eq_ignore_ascii_case(b"SETEX")
                || command.eq_ignore_ascii_case(b"PSETEX")
                || command.eq_ignore_ascii_case(b"SETRANGE")
                || command.eq_ignore_ascii_case(b"SETBIT")
                || command.eq_ignore_ascii_case(b"SADD")
                || command.eq_ignore_ascii_case(b"SREM")
                || command.eq_ignore_ascii_case(b"SPOP")
        }
        b'G' => {
            command.eq_ignore_ascii_case(b"GETDEL")
                || command.eq_ignore_ascii_case(b"GETSET")
                || command.eq_ignore_ascii_case(b"GETEX")
                || command.eq_ignore_ascii_case(b"GEOADD")
        }
        b'I' => {
            command.eq_ignore_ascii_case(b"INCR")
                || command.eq_ignore_ascii_case(b"INCRBY")
                || command.eq_ignore_ascii_case(b"INCRBYFLOAT")
        }
        b'D' => command.eq_ignore_ascii_case(b"DECR") || command.eq_ignore_ascii_case(b"DECRBY"),
        b'A' => command.eq_ignore_ascii_case(b"APPEND"),
        b'P' => command.eq_ignore_ascii_case(b"PERSIST") || command.eq_ignore_ascii_case(b"PFADD"),
        b'H' => {
            command.eq_ignore_ascii_case(b"HSET")
                || command.eq_ignore_ascii_case(b"HSETNX")
                || command.eq_ignore_ascii_case(b"HMSET")
                || command.eq_ignore_ascii_case(b"HDEL")
                || command.eq_ignore_ascii_case(b"HINCRBY")
                || command.eq_ignore_ascii_case(b"HINCRBYFLOAT")
        }
        b'L' => {
            command.eq_ignore_ascii_case(b"LPUSH")
                || command.eq_ignore_ascii_case(b"LPOP")
                || command.eq_ignore_ascii_case(b"LSET")
                || command.eq_ignore_ascii_case(b"LTRIM")
                || command.eq_ignore_ascii_case(b"LREM")
                || command.eq_ignore_ascii_case(b"LINSERT")
        }
        b'R' => command.eq_ignore_ascii_case(b"RPUSH") || command.eq_ignore_ascii_case(b"RPOP"),
        b'Z' => {
            command.eq_ignore_ascii_case(b"ZADD")
                || command.eq_ignore_ascii_case(b"ZREM")
                || command.eq_ignore_ascii_case(b"ZINCRBY")
                || command.eq_ignore_ascii_case(b"ZPOPMIN")
                || command.eq_ignore_ascii_case(b"ZPOPMAX")
        }
        b'J' => {
            command.eq_ignore_ascii_case(b"JSON.SET")
                || command.eq_ignore_ascii_case(b"JSON.DEL")
                || command.eq_ignore_ascii_case(b"JSON.NUMINCRBY")
                || command.eq_ignore_ascii_case(b"JSON.NUMMULTBY")
                || command.eq_ignore_ascii_case(b"JSON.STRAPPEND")
                || command.eq_ignore_ascii_case(b"JSON.ARRAPPEND")
                || command.eq_ignore_ascii_case(b"JSON.ARRINSERT")
                || command.eq_ignore_ascii_case(b"JSON.ARRPOP")
                || command.eq_ignore_ascii_case(b"JSON.ARRTRIM")
                || command.eq_ignore_ascii_case(b"JSON.TOGGLE")
                || command.eq_ignore_ascii_case(b"JSON.CLEAR")
        }
        b'X' => {
            command.eq_ignore_ascii_case(b"XADD")
                || command.eq_ignore_ascii_case(b"XTRIM")
                || command.eq_ignore_ascii_case(b"XDEL")
                || command.eq_ignore_ascii_case(b"XGROUP")
                || command.eq_ignore_ascii_case(b"XACK")
        }
        _ => false,
    };
    if is_mutation { Some(1) } else { None }
}

fn capture_command_mutation(conn: &mut Conn, parts: &[&str]) {
    let Some(command) = parts.first() else { return };
    let command = command.as_bytes();

    if let Some(key_index) = mutation_key_index(command) {
        if let Some(key) = parts.get(key_index) {
            conn.store.record_current_value(key);
        }
        return;
    }

    let capture = |conn: &Conn, index: usize| {
        if let Some(key) = parts.get(index) {
            conn.store.record_current_value(key);
        }
    };

    if command.eq_ignore_ascii_case(b"MSET") || command.eq_ignore_ascii_case(b"MSETNX") {
        for index in (1..parts.len()).step_by(2) {
            capture(conn, index);
        }
    } else if command.eq_ignore_ascii_case(b"RENAME")
        || command.eq_ignore_ascii_case(b"RENAMENX")
        || command.eq_ignore_ascii_case(b"SMOVE")
        || command.eq_ignore_ascii_case(b"LMOVE")
        || command.eq_ignore_ascii_case(b"RPOPLPUSH")
    {
        capture(conn, 1);
        capture(conn, 2);
    } else if command.eq_ignore_ascii_case(b"COPY") || command.eq_ignore_ascii_case(b"BITOP") {
        capture(conn, 2);
    }
}

#[inline(always)]
pub fn cmd_eq(a: &[u8], upper: &[u8]) -> bool {
    a.len() == upper.len()
        && a.iter()
            .zip(upper.iter())
            .all(|(&ac, &uc)| ac.to_ascii_uppercase() == uc)
}
