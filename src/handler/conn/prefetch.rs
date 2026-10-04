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
        n = n.checked_mul(10)?.checked_add((b - b'0') as usize)?;
        i += 1;
    }
    None
}

fn names_key(c: &[u8]) -> bool {
    const NON_KEY: &[&[u8]] = &[
        b"PUBLISH", b"SPUBLISH", b"SUBSCRIBE", b"PSUBSCRIBE", b"UNSUBSCRIBE", b"PUNSUBSCRIBE", b"PING",
        b"ECHO", b"AUTH", b"SELECT", b"HELLO", b"CLIENT", b"CONFIG", b"INFO", b"CLUSTER", b"COMMAND",
        b"MULTI", b"EXEC", b"FT.SEARCH", b"FT.AGGREGATE", b"FT.HYBRID", b"FT.CREATE", b"FT.INFO",
    ];
    !NON_KEY.iter().any(|r| r.eq_ignore_ascii_case(c))
}

pub(super) fn scan_keys<'a>(buf: &'a [u8], out: &mut [&'a [u8]; BATCH]) -> (usize, usize) {
    let mut count = 0;
    let mut pos = 0;
    let mut scanned = 0;
    while scanned < BATCH && pos < buf.len() && buf[pos] == b'*' {
        scanned += 1;
        let Some((argc, mut i)) = read_int(buf, pos + 1) else {
            break;
        };
        if argc > 64 {
            break;
        }
        let mut key: Option<&'a [u8]> = None;
        let mut read = false;
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
            let Some(end) = body.checked_add(len) else {
                ok = false;
                break;
            };
            if end > buf.len().saturating_sub(2) {
                ok = false;
                break;
            }
            if arg == 0 {
                read = names_key(&buf[body..end]);
            } else if arg == 1 && read {
                key = Some(&buf[body..end]);
            }
            i = end + 2;
        }
        if !ok {
            break;
        }
        if let Some(k) = key
            && (count == 0 || out[count - 1] != k)
        {
            out[count] = k;
            count += 1;
        }
        pos = i;
    }
    (count, pos)
}

pub(super) fn peek_cmd_key(buf: &[u8]) -> Option<(&[u8], &[u8])> {
    if buf.first() != Some(&b'*') {
        return None;
    }
    let (argc, mut i) = read_int(buf, 1)?;
    if argc < 2 {
        return None;
    }
    let mut take = || -> Option<&[u8]> {
        if buf.get(i) != Some(&b'$') {
            return None;
        }
        let (len, body) = read_int(buf, i + 1)?;
        let end = body.checked_add(len)?;
        if end > buf.len().saturating_sub(2) {
            return None;
        }
        i = end + 2;
        Some(&buf[body..end])
    };
    let cmd = take()?;
    let key = take()?;
    Some((cmd, key))
}

#[derive(Clone, Copy, Default)]
pub(crate) struct Hint {
    ptr: usize,
    len: usize,
    hash: u64,
}

impl Conn<'_> {
    #[inline]
    pub(super) fn prefetch_ahead(&mut self) -> usize {
        self.hint_n = 0;
        let p = &self.parser;
        let buf = &p.rbuf[p.pos..p.filled];
        if buf.len() < 16 {
            return p.filled;
        }
        let mut keys: [&[u8]; BATCH] = [&[]; BATCH];
        let (n, used) = scan_keys(buf, &mut keys);
        if n < 2 {
            self.pf_backoff = (self.pf_backoff + 1).min(5);
            return p.pos + (used.max(1) << self.pf_backoff);
        }
        self.pf_backoff = 0;
        let next = p.pos + used.max(1);
        {
            let mut hashes = [0u64; BATCH];
            self.store.data.prefetch_batch(&keys[..n], &mut hashes);
            for (h, (k, hash)) in self.hints.iter_mut().zip(keys[..n].iter().zip(hashes)) {
                *h = Hint { ptr: k.as_ptr() as usize, len: k.len(), hash };
            }
            self.hint_n = n as u8;
        }
        next
    }

    pub(super) fn next_is_same_write(&self, raw: &[(*const u8, usize)]) -> bool {
        let p = &self.parser;
        let Some((cmd, key)) = peek_cmd_key(&p.rbuf[p.pos..p.filled]) else {
            return true;
        };
        let (cur_cmd, cur_key) = unsafe { (part_bytes(raw[0]), part_bytes(raw[1])) };
        cmd.eq_ignore_ascii_case(cur_cmd) && key == cur_key
    }

    #[inline]
    pub(super) fn hint_for(&self, part: (*const u8, usize)) -> Option<u64> {
        self.hints[..self.hint_n as usize]
            .iter()
            .find(|h| h.ptr == part.0 as usize && h.len == part.1 && h.hash != 0)
            .map(|h| h.hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scans_pipelined_keys_and_stops_at_partial() {
        let buf = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n*3\r\n$3\r\nSET\r\n$2\r\nab\r\n$1\r\nv\r\n*2\r\n$4\r\nhget\r\n$2\r\nhk\r\n*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$5\r\nhel";
        let mut out: [&[u8]; BATCH] = [&[]; BATCH];
        let (n, used) = scan_keys(buf, &mut out);
        assert_eq!(n, 3);
        assert_eq!(out[0], b"foo");
        assert_eq!(out[1], b"ab");
        assert_eq!(out[2], b"hk");
        assert_eq!(&buf[used..used + 4], b"*2\r\n");
    }

    #[test]
    fn collapses_runs_of_the_same_key() {
        let buf = b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n*2\r\n$4\r\nINCR\r\n$1\r\nk\r\n*2\r\n$3\r\nGET\r\n$1\r\nz\r\n";
        let mut out: [&[u8]; BATCH] = [&[]; BATCH];
        let (n, _) = scan_keys(buf, &mut out);
        assert_eq!(n, 2);
        assert_eq!((out[0], out[1]), (&b"k"[..], &b"z"[..]));
    }

    #[test]
    fn peeks_command_and_key() {
        let buf = b"*3\r\n$3\r\nSET\r\n$2\r\nab\r\n$1\r\nv\r\n";
        assert_eq!(peek_cmd_key(buf), Some((&b"SET"[..], &b"ab"[..])));
        assert_eq!(peek_cmd_key(b"*3\r\n$3\r\nSET\r\n$2\r\na"), None);
        assert_eq!(peek_cmd_key(b"PING\r\n"), None);
    }

    #[test]
    fn rejects_inline_and_garbage() {
        let mut out: [&[u8]; BATCH] = [&[]; BATCH];
        assert_eq!(scan_keys(b"PING\r\n", &mut out), (0, 0));
        assert_eq!(scan_keys(b"*x\r\n", &mut out), (0, 0));
        assert_eq!(scan_keys(b"*2\r\n$99999999999999\r\n", &mut out), (0, 0));
    }
}
