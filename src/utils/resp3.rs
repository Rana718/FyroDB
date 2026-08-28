pub const RESP3_NULL: &[u8] = b"_\r\n";
pub const RESP3_TRUE: &[u8] = b"#t\r\n";
pub const RESP3_FALSE: &[u8] = b"#f\r\n";

#[inline]
pub fn write_null(out: &mut Vec<u8>) {
    out.extend_from_slice(RESP3_NULL);
}

#[inline]
pub fn write_boolean_r3(out: &mut Vec<u8>, b: bool) {
    out.extend_from_slice(if b { RESP3_TRUE } else { RESP3_FALSE });
}

#[inline]
pub fn write_double(out: &mut Vec<u8>, f: f64) {
    out.push(b',');
    let s = if f == f64::INFINITY {
        "inf".to_string()
    } else if f == f64::NEG_INFINITY {
        "-inf".to_string()
    } else {
        format!("{}", f)
    };
    out.extend_from_slice(s.as_bytes());
    out.extend_from_slice(b"\r\n");
}

#[inline]
pub fn write_big_number(out: &mut Vec<u8>, n: &str) {
    out.push(b'(');
    out.extend_from_slice(n.as_bytes());
    out.extend_from_slice(b"\r\n");
}

#[inline]
pub fn write_verbatim_string(out: &mut Vec<u8>, encoding: &str, data: &str) {
    let total_len = encoding.len() + 1 + data.len();
    out.push(b'=');
    super::resp::write_usize(out, total_len);
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(encoding.as_bytes());
    out.push(b':');
    out.extend_from_slice(data.as_bytes());
    out.extend_from_slice(b"\r\n");
}

#[inline]
pub fn write_map_header(out: &mut Vec<u8>, len: usize) {
    out.push(b'%');
    super::resp::write_usize(out, len);
    out.extend_from_slice(b"\r\n");
}

#[inline]
pub fn write_set_header(out: &mut Vec<u8>, len: usize) {
    out.push(b'~');
    super::resp::write_usize(out, len);
    out.extend_from_slice(b"\r\n");
}

#[inline]
pub fn write_push_header(out: &mut Vec<u8>, len: usize) {
    out.push(b'>');
    super::resp::write_usize(out, len);
    out.extend_from_slice(b"\r\n");
}

#[inline]
pub fn write_attribute_header(out: &mut Vec<u8>, len: usize) {
    out.push(b'|');
    super::resp::write_usize(out, len);
    out.extend_from_slice(b"\r\n");
}

pub fn write_hello_resp3(out: &mut Vec<u8>) {
    write_map_header(out, 7);
    super::resp::write_bulk(out, "server");
    super::resp::write_bulk(out, "fyrodb");
    super::resp::write_bulk(out, "version");
    super::resp::write_bulk(out, env!("CARGO_PKG_VERSION"));
    super::resp::write_bulk(out, "proto");
    super::resp::write_integer(out, 3);
    super::resp::write_bulk(out, "id");
    super::resp::write_integer(out, 1);
    super::resp::write_bulk(out, "mode");
    super::resp::write_bulk(out, "standalone");
    super::resp::write_bulk(out, "role");
    super::resp::write_bulk(out, "master");
    super::resp::write_bulk(out, "modules");
    write_set_header(out, 0);
}

#[inline]
pub fn write_integer(out: &mut Vec<u8>, n: i64) {
    super::resp::write_integer(out, n);
}
