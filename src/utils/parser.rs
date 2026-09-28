pub struct RespParser {
    pub rbuf: Vec<u8>,
    pub filled: usize,
    pub pos: usize,
    pub wbuf: Vec<u8>,
    pub parts_raw: Vec<(*const u8, usize)>,
    /// Progress through a large command that arrived across several reads.
    /// Offsets are relative to the command start, which survives the
    /// compaction in `read_buf` (it only ever shifts the unparsed tail).
    partial: Option<Partial>,
}

struct Partial {
    count: usize,
    next: usize,
    parts: Vec<(usize, usize)>,
}

unsafe impl Send for RespParser {}

pub enum ParseResult {
    Complete,
    Incomplete,
    Error,
}

/// Redis accepts multibulk headers up to INT_MAX; 16M elements covers any
/// real HSET/MSET/SADD batch while bounding a hostile header.
const MAX_ARRAY_ELEMENTS: usize = 1 << 24;
const MAX_BULK_BYTES: usize = 512 * 1024 * 1024;
/// Upfront reservation cap: the header's count is untrusted until the
/// elements actually arrive.
const MAX_PREALLOC_PARTS: usize = 1024;
/// Commands with more elements than this save progress on `Incomplete`
/// instead of re-parsing from the start on every read.
const RESUME_THRESHOLD: usize = 64;

impl Default for RespParser {
    fn default() -> Self {
        Self::new()
    }
}

impl RespParser {
    /// Idle size of the read buffer, and the size it is reclaimed to.
    const IDLE_READ_BUFFER: usize = 2 * 1024;

    /// Buffers start unallocated and are created on first use.
    pub fn new() -> Self {
        Self {
            rbuf: Vec::new(),
            filled: 0,
            pos: 0,
            wbuf: Vec::new(),
            parts_raw: Vec::new(),
            partial: None,
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

    #[inline]
    pub fn peek_remaining(&self, max: usize) -> &[u8] {
        let avail = self.filled - self.pos;
        let n = avail.min(max);
        &self.rbuf[self.pos..self.pos + n]
    }

    pub fn parse_one(&mut self) -> ParseResult {
        self.parts_raw.clear();
        if self.partial.is_some() {
            return self.resume();
        }

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

        self.parts_raw.reserve(count.min(MAX_PREALLOC_PARTS));

        for i in 0..count {
            let elem_start = self.pos;
            match self.parse_bulk() {
                Ok(Some(part)) => self.parts_raw.push(part),
                Ok(None) => {
                    if count > RESUME_THRESHOLD {
                        self.save_partial(start_pos, count, i, elem_start);
                    }
                    self.pos = start_pos;
                    return ParseResult::Incomplete;
                }
                Err(()) => return ParseResult::Error,
            }
        }

        ParseResult::Complete
    }

    /// One `$len\r\n<bytes>\r\n` element at `self.pos`; `Ok(None)` when it
    /// has not fully arrived.
    #[inline(always)]
    fn parse_bulk(&mut self) -> Result<Option<(*const u8, usize)>, ()> {
        let Some((bs, be)) = self.scan_line() else {
            return Ok(None);
        };
        if self.rbuf.get(bs) != Some(&b'$') {
            return Err(());
        }
        let len = parse_usize(&self.rbuf[bs + 1..be], MAX_BULK_BYTES).ok_or(())?;
        if self.filled - self.pos < len + 2 {
            return Ok(None);
        }
        let ptr = self.rbuf[self.pos..].as_ptr();
        self.pos += len + 2;
        Ok(Some((ptr, len)))
    }

    fn save_partial(&mut self, start: usize, count: usize, next: usize, resume_at: usize) {
        let base = self.rbuf.as_ptr() as usize + start;
        let mut parts = Vec::with_capacity(self.parts_raw.len());
        for &(p, len) in &self.parts_raw {
            parts.push((p as usize - base, len));
        }
        parts.push((resume_at - start, usize::MAX));
        self.parts_raw.clear();
        self.partial = Some(Partial { count, next, parts });
    }

    /// Continue a large command from its saved element index. `self.pos`
    /// is the command start (reset on every `Incomplete`).
    fn resume(&mut self) -> ParseResult {
        let start = self.pos;
        let mut p = self.partial.take().expect("resume without partial state");
        let (resume_rel, _) = p.parts.pop().expect("partial carries a resume marker");
        self.pos = start + resume_rel;
        for i in p.next..p.count {
            let elem_start = self.pos;
            match self.parse_bulk() {
                Ok(Some((ptr, len))) => {
                    p.parts.push((ptr as usize - (self.rbuf.as_ptr() as usize + start), len))
                }
                Ok(None) => {
                    p.next = i;
                    p.parts.push((elem_start - start, usize::MAX));
                    self.partial = Some(p);
                    self.pos = start;
                    return ParseResult::Incomplete;
                }
                Err(()) => return ParseResult::Error,
            }
        }
        let base = self.rbuf[start..].as_ptr();
        self.parts_raw.reserve(p.parts.len());
        for (off, len) in p.parts {
            // SAFETY: every offset was produced from a live element inside
            // `rbuf[start..self.pos]`, which has not moved since this call
            // began and will not move until the next `read_buf`.
            self.parts_raw.push((unsafe { base.add(off) }, len));
        }
        ParseResult::Complete
    }

    /// Return an oversized read buffer to its idle size.
    #[inline(always)]
    pub fn in_partial(&self) -> bool {
        self.partial.is_some()
    }

    pub fn release_read_buffer(&mut self) {
        if self.pos == self.filled && self.partial.is_none() && self.rbuf.len() > 16 * 1024 {
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

    /// Regression: `parts_raw` pointers must remain valid after `parse_one`
    /// returns `Complete` — `rbuf` must not be reallocated until dispatch is
    #[test]
    fn large_command_parts_stay_inside_the_live_read_buffer() {
        let mut parser = RespParser::new();
        let value = "x".repeat(20_000);
        let command = format!(
            "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n${}\r\n{value}\r\n",
            value.len()
        );
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

    #[test]
    fn a_fresh_parser_allocates_nothing() {
        let parser = RespParser::new();
        assert_eq!(parser.rbuf.capacity(), 0);
        assert_eq!(parser.wbuf.capacity(), 0);
        assert_eq!(parser.parts_raw.capacity(), 0);
    }

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

    fn big_hset(fields: usize) -> (Vec<u8>, Vec<String>) {
        let mut args = vec!["HSET".to_string(), "h".to_string()];
        for i in 0..fields {
            args.push(format!("field:{i}"));
            args.push(format!("value-{i}"));
        }
        let mut wire = format!("*{}\r\n", args.len()).into_bytes();
        for a in &args {
            wire.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
        }
        (wire, args)
    }

    fn parts_of(parser: &RespParser) -> Vec<String> {
        parser
            .parts_raw
            .iter()
            .map(|&(p, l)| {
                String::from_utf8(unsafe { std::slice::from_raw_parts(p, l) }.to_vec()).unwrap()
            })
            .collect()
    }

    #[test]
    fn commands_beyond_1024_elements_parse() {
        let (wire, args) = big_hset(5000);
        let mut parser = RespParser::new();
        feed(&mut parser, &wire);
        assert!(matches!(parser.parse_one(), ParseResult::Complete));
        assert_eq!(parts_of(&parser), args);
    }

    /// A large command trickling in over many reads resumes from its saved
    /// element index and still yields every part intact, including when
    /// `read_buf` compacts or grows the buffer between reads.
    #[test]
    fn a_fragmented_large_command_resumes_across_reads() {
        let (mut wire, args) = big_hset(3000);
        wire.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
        let mut parser = RespParser::new();
        let mut offset = 0;
        let mut done = false;
        for chunk in [7usize, 1, 333, 4096, 17, 10_000].iter().cycle() {
            if offset >= wire.len() {
                break;
            }
            let buf = parser.read_buf();
            let n = (*chunk).min(buf.len()).min(wire.len() - offset);
            buf[..n].copy_from_slice(&wire[offset..offset + n]);
            parser.did_fill(n);
            offset += n;
            if !done {
                match parser.parse_one() {
                    ParseResult::Complete => {
                        assert_eq!(parts_of(&parser), args);
                        done = true;
                    }
                    ParseResult::Incomplete => {}
                    ParseResult::Error => panic!("parse error mid-stream"),
                }
            }
        }
        if !done {
            assert!(matches!(parser.parse_one(), ParseResult::Complete));
            assert_eq!(parts_of(&parser), args);
        }
        assert!(matches!(parser.parse_one(), ParseResult::Complete));
        assert_eq!(parts_of(&parser), vec!["GET", "k"]);
    }

    #[test]
    fn a_hostile_array_header_does_not_preallocate() {
        let mut parser = RespParser::new();
        feed(&mut parser, b"*16000000\r\n$3\r\nSET\r\n");
        assert!(matches!(parser.parse_one(), ParseResult::Incomplete));
        assert!(parser.parts_raw.capacity() <= 1024);
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
