pub struct RespParser {
    pub rbuf: Vec<u8>,
    pub filled: usize,
    pub pos: usize,
    pub wbuf: Vec<u8>,
    pub parts_raw: Vec<(*const u8, usize)>,
}

unsafe impl Send for RespParser {}

pub enum ParseResult {
    Complete,
    Incomplete,
    Error,
}

const MAX_ARRAY_ELEMENTS: usize = 1024;
const MAX_BULK_BYTES: usize = 512 * 1024 * 1024;

impl Default for RespParser {
    fn default() -> Self {
        Self::new()
    }
}

impl RespParser {
    /// Idle size of the read buffer, and the size it is reclaimed to.
    const IDLE_READ_BUFFER: usize = 2 * 1024;

    /// Buffers start unallocated and are created on first use.
    ///
    /// Every accepted connection used to commit ~3 KiB up front whether or not
    /// it ever sent a byte. That is invisible for one client and material at
    /// scale — and cluster mode multiplies it, since a cluster-aware client
    /// dials every node, so N clients become 3N server-side connections.
    pub fn new() -> Self {
        Self {
            rbuf: Vec::new(),
            filled: 0,
            pos: 0,
            wbuf: Vec::new(),
            parts_raw: Vec::new(),
        }
    }

    pub fn read_buf(&mut self) -> &mut [u8] {
        if self.rbuf.len() - self.filled < 1024 {
            if self.pos > 0 {
                self.rbuf.copy_within(self.pos..self.filled, 0);
                self.filled -= self.pos;
                self.pos = 0;
            }
            if self.rbuf.len() - self.filled < 1024 {
                // `len * 2` cannot grow an empty buffer, so the first read has
                // to establish the idle size.
                let grown = (self.rbuf.len() * 2).max(Self::IDLE_READ_BUFFER);
                self.rbuf.resize(grown, 0);
            }
        }
        &mut self.rbuf[self.filled..]
    }

    #[inline]
    pub fn did_fill(&mut self, n: usize) {
        self.filled += n;
    }

    pub fn parse_one(&mut self) -> ParseResult {
        self.parts_raw.clear();

        let start_pos = self.pos;

        let (s, e) = match self.scan_line() {
            Some(r) => r,
            None => {
                self.pos = start_pos;
                return ParseResult::Incomplete;
            }
        };

        if self.rbuf.get(s) != Some(&b'*') {
            return ParseResult::Error;
        }
        let count = match parse_usize(&self.rbuf[s + 1..e], MAX_ARRAY_ELEMENTS) {
            Some(c) => c,
            None => return ParseResult::Error,
        };

        self.parts_raw.reserve(count);

        for _ in 0..count {
            let (bs, be) = match self.scan_line() {
                Some(r) => r,
                None => {
                    self.pos = start_pos;
                    return ParseResult::Incomplete;
                }
            };
            if self.rbuf.get(bs) != Some(&b'$') {
                return ParseResult::Error;
            }
            let len = match parse_usize(&self.rbuf[bs + 1..be], MAX_BULK_BYTES) {
                Some(l) => l,
                None => return ParseResult::Error,
            };

            if self.filled - self.pos < len + 2 {
                self.pos = start_pos;
                return ParseResult::Incomplete;
            }

            let ptr = self.rbuf[self.pos..].as_ptr();
            self.parts_raw.push((ptr, len));
            self.pos += len + 2;
        }

        ParseResult::Complete
    }

    /// Return an oversized read buffer to its idle size.
    ///
    /// `parts_raw` holds raw pointers into `rbuf`, so this must only run once
    /// the caller is done dispatching the parsed command — shrinking here
    /// reallocates and would leave those pointers covering freed memory.
    pub fn release_read_buffer(&mut self) {
        if self.pos == self.filled && self.rbuf.len() > 16 * 1024 {
            self.parts_raw.clear();
            self.rbuf.truncate(Self::IDLE_READ_BUFFER);
            self.rbuf.shrink_to(Self::IDLE_READ_BUFFER);
            self.filled = 0;
            self.pos = 0;
        }
    }

    #[inline(always)]
    fn scan_line(&mut self) -> Option<(usize, usize)> {
        let rel = memchr::memchr(b'\n', &self.rbuf[self.pos..self.filled])?;
        let start = self.pos;
        let nl = self.pos + rel;
        self.pos = nl + 1;
        let end = if nl > start { nl - 1 } else { nl };
        Some((start, end))
    }
}

#[inline(always)]
fn parse_usize(s: &[u8], max: usize) -> Option<usize> {
    if s.is_empty() {
        return None;
    }
    if s.len() <= 3 {
        let mut n: usize = 0;
        for &b in s {
            if !b.is_ascii_digit() {
                return None;
            }
            n = n * 10 + (b - b'0') as usize;
        }
        if n > max {
            return None;
        }
        return Some(n);
    }
    let mut n: usize = 0;
    for &b in s {
        if !b.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?.checked_add((b - b'0') as usize)?;
        if n > max {
            return None;
        }
    }
    Some(n)
}

#[cfg(test)]
mod tests {
    use super::{ParseResult, RespParser};

    fn feed(parser: &mut RespParser, bytes: &[u8]) {
        let mut offset = 0;
        while offset < bytes.len() {
            let buf = parser.read_buf();
            let n = buf.len().min(bytes.len() - offset);
            buf[..n].copy_from_slice(&bytes[offset..offset + n]);
            parser.did_fill(n);
            offset += n;
        }
    }

    /// `parse_one` used to shrink `rbuf` before returning `Complete`, leaving
    /// every pointer in `parts_raw` covering freed memory. A single command
    /// large enough to grow `rbuf` past 16KiB reproduced it.
    #[test]
    fn large_command_parts_stay_inside_the_live_read_buffer() {
        let mut parser = RespParser::new();
        let value = "x".repeat(20_000);
        let command = format!("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n${}\r\n{value}\r\n", value.len());
        feed(&mut parser, command.as_bytes());

        assert!(matches!(parser.parse_one(), ParseResult::Complete));

        let base = parser.rbuf.as_ptr() as usize;
        let live = parser.rbuf.capacity();
        assert_eq!(parser.parts_raw.len(), 3);
        for &(ptr, len) in &parser.parts_raw {
            let offset = ptr as usize - base;
            assert!(
                offset + len <= live,
                "part at offset {offset} len {len} escapes the {live} byte buffer"
            );
        }
        let (ptr, len) = parser.parts_raw[2];
        let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
        assert_eq!(bytes, value.as_bytes());
    }

    #[test]
    fn releasing_the_read_buffer_reclaims_it_after_a_large_command() {
        let mut parser = RespParser::new();
        let value = "y".repeat(20_000);
        let command = format!("*2\r\n$3\r\nGET\r\n${}\r\n{value}\r\n", value.len());
        feed(&mut parser, command.as_bytes());
        assert!(matches!(parser.parse_one(), ParseResult::Complete));
        assert!(parser.rbuf.len() > 16 * 1024);

        parser.release_read_buffer();
        assert_eq!(parser.rbuf.len(), 2 * 1024);
        assert!(parser.parts_raw.is_empty());
        assert_eq!(parser.pos, 0);
        assert_eq!(parser.filled, 0);
    }

    /// An accepted-but-silent connection should not commit buffer memory.
    #[test]
    fn a_fresh_parser_allocates_nothing() {
        let parser = RespParser::new();
        assert_eq!(parser.rbuf.capacity(), 0);
        assert_eq!(parser.wbuf.capacity(), 0);
        assert_eq!(parser.parts_raw.capacity(), 0);
    }

    /// The first read still has to produce a usable buffer; doubling an empty
    /// one would loop forever at zero length.
    #[test]
    fn the_first_read_establishes_the_idle_buffer_size() {
        let mut parser = RespParser::new();
        let buf = parser.read_buf();
        assert!(
            buf.len() >= 1024,
            "first read_buf handed back only {} bytes",
            buf.len()
        );
        assert_eq!(parser.rbuf.len(), RespParser::IDLE_READ_BUFFER);
    }

    #[test]
    fn a_lazy_parser_still_parses_a_pipeline() {
        let mut parser = RespParser::new();
        let mut wire = Vec::new();
        for i in 0..64 {
            let key = format!("key{i}");
            wire.extend_from_slice(
                format!("*2\r\n$3\r\nGET\r\n${}\r\n{key}\r\n", key.len()).as_bytes(),
            );
        }
        feed(&mut parser, &wire);
        let mut parsed = 0;
        while let ParseResult::Complete = parser.parse_one() {
            assert_eq!(parser.parts_raw.len(), 2);
            parsed += 1;
        }
        assert_eq!(parsed, 64);
    }
}
