use super::*;

const BATCH: usize = customhash::PREFETCH_BATCH;

#[inline]
fn read_int(buf: &[u8], mut i: usize) -> Option<(usize, usize)> {
    let mut n = 0usize;
    let start = i;
    while i < buf.len() {
        let b = buf[i];
        if b == b'\r' {
            if i == start || i + 1 >= buf.len() || buf[i + 1] != b'\n' {
                return None;
            }
            return Some((n, i + 2));
        }
        if !b.is_ascii_digit() || i - start >= 12 {
            return None;
        }
        n = n * 10 + (b - b'0') as usize;
        i += 1;
    }
    None
}

pub(super) fn scan_keys<'a>(buf: &'a [u8], out: &mut [&'a [u8]; BATCH]) -> (usize, usize) {
    let mut count = 0;
    let mut pos = 0;
    while count < BATCH && pos < buf.len() && buf[pos] == b'*' {
        let Some((argc, mut i)) = read_int(buf, pos + 1) else {
            break;
        };
        if argc > 64 {
            break;
        }
        let mut key: Option<&'a [u8]> = None;
        let mut ok = true;
        for arg in 0..argc {
            if i >= buf.len() || buf[i] != b'$' {
                ok = false;
                break;
            }
            let Some((len, body)) = read_int(buf, i + 1) else {
                ok = false;
                break;
            };
            let end = body + len;
            if end + 2 > buf.len() {
                ok = false;
                break;
            }
            if arg == 1 {
                key = Some(&buf[body..end]);
            }
            i = end + 2;
        }
        if !ok {
            break;
        }
        if let Some(k) = key {
            out[count] = k;
            count += 1;
        }
        pos = i;
    }
    (count, pos)
}

impl Conn<'_> {
    #[inline]
    pub(super) fn prefetch_ahead(&self) -> usize {
        let p = &self.parser;
        let buf = &p.rbuf[p.pos..p.filled];
        if buf.len() < 16 {
            return p.filled;
        }
        let mut keys: [&[u8]; BATCH] = [&[]; BATCH];
        let (n, used) = scan_keys(buf, &mut keys);
        if n >= 2 {
            self.store.data.prefetch_batch(&keys[..n]);
        }
        p.pos + used.max(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scans_pipelined_keys_and_stops_at_partial() {
        let buf = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n*3\r\n$3\r\nSET\r\n$2\r\nab\r\n$1\r\nv\r\n*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$5\r\nhel";
        let mut out: [&[u8]; BATCH] = [&[]; BATCH];
        let (n, used) = scan_keys(buf, &mut out);
        assert_eq!(n, 2);
        assert_eq!(out[0], b"foo");
        assert_eq!(out[1], b"ab");
        assert_eq!(&buf[used..used + 4], b"*2\r\n");
    }

    #[test]
    fn rejects_inline_and_garbage() {
        let mut out: [&[u8]; BATCH] = [&[]; BATCH];
        assert_eq!(scan_keys(b"PING\r\n", &mut out), (0, 0));
        assert_eq!(scan_keys(b"*x\r\n", &mut out), (0, 0));
        assert_eq!(scan_keys(b"*2\r\n$99999999999999\r\n", &mut out), (0, 0));
    }
}
