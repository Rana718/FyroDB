use crate::storage::store::Store;
use crate::utils::resp;

pub fn scan(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let args = match parts {
        [_, rest @ ..] if !rest.is_empty() => rest,
        _ => return resp::write_wrong_args(out, "scan"),
    };

    let cursor = match args[0].parse::<usize>() {
        Ok(c) => c,
        Err(_) => return resp::write_err(out, "value is not an integer or out of range"),
    };

    let mut pattern: Option<&str> = None;
    let mut count: usize = 10;
    let mut i = 1;

    while i < args.len() {
        let opt = args[i].as_bytes();
        if opt.eq_ignore_ascii_case(b"MATCH") {
            i += 1;
            if i >= args.len() {
                return resp::write_err(out, "syntax error");
            }
            pattern = Some(args[i]);
        } else if opt.eq_ignore_ascii_case(b"COUNT") {
            i += 1;
            if i >= args.len() {
                return resp::write_err(out, "syntax error");
            }
            count = match args[i].parse::<usize>() {
                Ok(c) if c > 0 => c,
                _ => return resp::write_err(out, "value is not an integer or out of range"),
            };
        } else {
            return resp::write_err(out, "syntax error");
        }
        i += 1;
    }

    let (next_cursor, keys) = store.scan(cursor, pattern, count);
    out.extend_from_slice(b"*2\r\n");
    let cursor_str = next_cursor.to_string();
    resp::write_bulk(out, &cursor_str);
    resp::write_array(out, &keys);
}
