pub const OK: &[u8] = b"+OK\r\n";
pub const NIL: &[u8] = b"$-1\r\n";
pub const ZERO: &[u8] = b":0\r\n";
pub const ONE: &[u8] = b":1\r\n";
pub const PONG: &[u8] = b"+PONG\r\n";

static INT_CACHE: [&[u8]; 10] = [
    b":0\r\n", b":1\r\n", b":2\r\n", b":3\r\n", b":4\r\n", b":5\r\n", b":6\r\n", b":7\r\n",
    b":8\r\n", b":9\r\n",
];

#[inline]
pub fn write_ok(out: &mut Vec<u8>) {
    out.extend_from_slice(OK);
}

#[inline]
pub fn write_pong(out: &mut Vec<u8>) {
    out.extend_from_slice(PONG);
}

#[inline]
pub fn write_nil(out: &mut Vec<u8>) {
    out.extend_from_slice(NIL);
}

#[inline]
pub fn write_bulk(out: &mut Vec<u8>, s: &str) {
    write_bulk_bytes(out, s.as_bytes());
}

#[inline]
pub fn write_bulk_bytes(out: &mut Vec<u8>, s: &[u8]) {
    let len = s.len();
    out.reserve(1 + 5 + 2 + len + 2);
    match len {
        0..10 => {
            out.push(b'$');
            out.push(b'0' + len as u8);
            out.extend_from_slice(b"\r\n");
        }
        10..100 => {
            out.push(b'$');
            out.push(b'0' + (len / 10) as u8);
            out.push(b'0' + (len % 10) as u8);
            out.extend_from_slice(b"\r\n");
        }
        _ => {
            out.push(b'$');
            write_usize(out, len);
            out.extend_from_slice(b"\r\n");
        }
    }
    out.extend_from_slice(s);
    out.extend_from_slice(b"\r\n");
}

#[inline]
pub fn write_integer(out: &mut Vec<u8>, n: i64) {
    if (0..10).contains(&n) {
        out.extend_from_slice(INT_CACHE[n as usize]);
        return;
    }
    out.push(b':');
    write_i64(out, n);
    out.extend_from_slice(b"\r\n");
}

#[inline]
pub fn write_boolean(out: &mut Vec<u8>, b: bool) {
    out.extend_from_slice(if b { ONE } else { ZERO });
}

#[inline]
pub fn write_opt_bulk(out: &mut Vec<u8>, v: Option<String>) {
    match v {
        Some(s) => write_bulk(out, &s),
        None => write_nil(out),
    }
}

#[inline]
pub fn write_opt_bulk_ref(out: &mut Vec<u8>, v: Option<&str>) {
    match v {
        Some(s) => write_bulk(out, s),
        None => write_nil(out),
    }
}

#[inline]
pub fn write_array_header(out: &mut Vec<u8>, len: usize) {
    out.push(b'*');
    write_usize(out, len);
    out.extend_from_slice(b"\r\n");
}

#[inline]
pub fn write_array(out: &mut Vec<u8>, items: &[String]) {
    write_array_header(out, items.len());
    for item in items {
        write_bulk(out, item);
    }
}

#[inline]
pub fn write_opt_array(out: &mut Vec<u8>, items: &[Option<String>]) {
    write_array_header(out, items.len());
    for item in items {
        match item {
            Some(s) => write_bulk(out, s),
            None => write_nil(out),
        }
    }
}

#[inline]
pub fn write_err(out: &mut Vec<u8>, msg: &str) {
    out.extend_from_slice(b"-ERR ");
    out.extend_from_slice(msg.as_bytes());
    out.extend_from_slice(b"\r\n");
}

#[inline]
pub fn write_store_err(out: &mut Vec<u8>, err: &str) {
    if err == "WRONGTYPE" {
        write_wrong_type(out);
        return;
    }
    let code = err.split(' ').next().unwrap_or("");
    if code.len() >= 3 && err.len() > code.len() && code.bytes().all(|b| b.is_ascii_uppercase()) {
        out.push(b'-');
        out.extend_from_slice(err.as_bytes());
        out.extend_from_slice(b"\r\n");
    } else {
        write_err(out, err);
    }
}

#[inline]
pub fn write_wrong_type(out: &mut Vec<u8>) {
    out.extend_from_slice(
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    );
}

#[inline]
pub fn write_wrong_args(out: &mut Vec<u8>, cmd: &str) {
    out.extend_from_slice(b"-ERR wrong number of arguments for '");
    out.extend_from_slice(cmd.as_bytes());
    out.extend_from_slice(b"' command\r\n");
}

#[inline]
pub fn write_simple(out: &mut Vec<u8>, s: &str) {
    out.push(b'+');
    out.extend_from_slice(s.as_bytes());
    out.extend_from_slice(b"\r\n");
}

#[inline(always)]
pub fn write_usize(out: &mut Vec<u8>, mut n: usize) {
    if n == 0 {
        out.push(b'0');
        return;
    }
    let start = out.len();
    while n > 0 {
        out.push(b'0' + (n % 10) as u8);
        n /= 10;
    }
    out[start..].reverse();
}

#[inline(always)]
pub fn write_i64(out: &mut Vec<u8>, mut n: i64) {
    if n < 0 {
        out.push(b'-');
        let start = out.len();
        loop {
            out.push(b'0' + (-(n % 10)) as u8);
            n /= 10;
            if n == 0 {
                break;
            }
        }
        out[start..].reverse();
    } else {
        write_usize(out, n as usize);
    }
}

#[inline]
pub fn bulk(s: &str) -> String {
    format!("${}\r\n{}\r\n", s.len(), s)
}
