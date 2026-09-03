//! Caps on key count (Redis denyoom semantics): keyspace-growing writes
//! are refused at the limit; freeing commands stay available.

/// True for keyspace-growing writes; removals must stay available so a
/// client can free room.
pub fn is_denyoom_command(command: &[u8]) -> bool {
    crate::cluster::is_write_command(command) && !frees_memory(command)
}

/// Commands that only shrink the dataset. Kept permissive: misclassifying a
/// shrinking command as `denyoom` would leave a full store with no way out.
fn frees_memory(command: &[u8]) -> bool {
    let Some(&first) = command.first() else {
        return false;
    };
    match first.to_ascii_uppercase() {
        b'D' => {
            command.eq_ignore_ascii_case(b"DEL")
                || command.eq_ignore_ascii_case(b"DECR")
                || command.eq_ignore_ascii_case(b"DECRBY")
        }
        b'U' => command.eq_ignore_ascii_case(b"UNLINK"),
        b'F' => {
            command.eq_ignore_ascii_case(b"FLUSHDB") || command.eq_ignore_ascii_case(b"FLUSHALL")
        }
        b'E' => {
            command.eq_ignore_ascii_case(b"EXPIRE")
                || command.eq_ignore_ascii_case(b"EXPIREAT")
        }
        b'P' => {
            command.eq_ignore_ascii_case(b"PEXPIRE")
                || command.eq_ignore_ascii_case(b"PEXPIREAT")
                || command.eq_ignore_ascii_case(b"PERSIST")
        }
        b'G' => command.eq_ignore_ascii_case(b"GETDEL"),
        b'H' => command.eq_ignore_ascii_case(b"HDEL"),
        b'S' => command.eq_ignore_ascii_case(b"SREM") || command.eq_ignore_ascii_case(b"SPOP"),
        b'Z' => {
            command.eq_ignore_ascii_case(b"ZREM")
                || command.eq_ignore_ascii_case(b"ZPOPMIN")
                || command.eq_ignore_ascii_case(b"ZPOPMAX")
        }
        b'L' => {
            command.eq_ignore_ascii_case(b"LPOP")
                || command.eq_ignore_ascii_case(b"LREM")
                || command.eq_ignore_ascii_case(b"LTRIM")
        }
        b'R' => command.eq_ignore_ascii_case(b"RPOP"),
        b'X' => command.eq_ignore_ascii_case(b"XDEL") || command.eq_ignore_ascii_case(b"XTRIM"),
        b'J' => {
            command.eq_ignore_ascii_case(b"JSON.DEL")
                || command.eq_ignore_ascii_case(b"JSON.FORGET")
                || command.eq_ignore_ascii_case(b"JSON.CLEAR")
                || command.eq_ignore_ascii_case(b"JSON.ARRPOP")
                || command.eq_ignore_ascii_case(b"JSON.ARRTRIM")
        }
        _ => false,
    }
}

/// Redis's reply when a `denyoom` command is refused.
pub const OOM_REPLY: &[u8] = b"-OOM command not allowed when used memory > 'maxmemory'.\r\n";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn growing_writes_are_gated() {
        for command in [
            &b"SET"[..],
            b"set",
            b"SETEX",
            b"PSETEX",
            b"SETNX",
            b"APPEND",
            b"INCR",
            b"LPUSH",
            b"RPUSH",
            b"SADD",
            b"HSET",
            b"ZADD",
            b"XADD",
            b"MSET",
            b"JSON.SET",
        ] {
            assert!(
                is_denyoom_command(command),
                "{} should be gated",
                String::from_utf8_lossy(command)
            );
        }
    }

    #[test]
    fn reads_and_shrinking_writes_are_never_gated() {
        for command in [
            &b"GET"[..],
            b"MGET",
            b"EXISTS",
            b"TTL",
            b"INFO",
            b"DEL",
            b"del",
            b"UNLINK",
            b"FLUSHALL",
            b"FLUSHDB",
            b"EXPIRE",
            b"PERSIST",
            b"GETDEL",
            b"HDEL",
            b"SREM",
            b"SPOP",
            b"ZREM",
            b"ZPOPMIN",
            b"LPOP",
            b"RPOP",
            b"LTRIM",
            b"XDEL",
            b"JSON.DEL",
        ] {
            assert!(
                !is_denyoom_command(command),
                "{} must stay available",
                String::from_utf8_lossy(command)
            );
        }
    }

    #[test]
    fn empty_command_is_not_gated() {
        assert!(!is_denyoom_command(b""));
    }
}
