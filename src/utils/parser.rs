pub struct RespParser {
    pub rbuf: Vec<u8>,
    pub filled: usize,
    pub pos: usize,
    pub wbuf: Vec<u8>,
    pub parts_raw: Vec<(*const u8, usize)>,
    /// Progress through multi-read commands with offsets relative to command start,
    /// surviving buffer compaction.
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
        if self.pos == self.filled && self.partial.is_none() {
            self.pos = 0;
            self.filled = 0;
        } else if self.rbuf.len() - self.filled < 1024 {
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
        if self.rbuf.is_empty() {
            self.rbuf.resize(Self::IDLE_READ_BUFFER, 0);
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

    #[inline(always)]
    fn try_fast_get_at(&mut self, buf: *const u8, avail: usize) -> Option<ParseResult> {
        const H2: u32 = u32::from_ne_bytes(*b"$3\r\n");
        const GET_UPPER: u32 = u32::from_ne_bytes(*b"GET\r");
        const GET_LOWER: u32 = u32::from_ne_bytes(*b"get\r");
        const LF_DOLLAR: u16 = u16::from_ne_bytes(*b"\n$");
        const CRLF: u16 = u16::from_ne_bytes(*b"\r\n");

        if avail < 20 {
            return None;
        }
        if unsafe { std::ptr::read_unaligned(buf.add(4) as *const u32) } != H2 {
            return None;
        }
        let w4 = unsafe { std::ptr::read_unaligned(buf.add(8) as *const u32) };
        if w4 != GET_UPPER && w4 != GET_LOWER {
            return None;
        }
        if unsafe { std::ptr::read_unaligned(buf.add(12) as *const u16) } != LF_DOLLAR {
            return None;
        }
        let k1 = unsafe { *buf.add(14) };
        if !k1.is_ascii_digit() {
            return None;
        }
        let (key_len, key_offset) = if unsafe { std::ptr::read_unaligned(buf.add(15) as *const u16) } == CRLF {
            ((k1 - b'0') as usize, 17)
        } else {
            let k2 = unsafe { *buf.add(15) };
            if k2.is_ascii_digit() {
                if unsafe { std::ptr::read_unaligned(buf.add(16) as *const u16) } == CRLF {
                    (((k1 - b'0') as usize) * 10 + (k2 - b'0') as usize, 18)
                } else {
                    let k3 = unsafe { *buf.add(16) };
                    if k3.is_ascii_digit() && unsafe { std::ptr::read_unaligned(buf.add(17) as *const u16) } == CRLF {
                        (((k1 - b'0') as usize) * 100 + ((k2 - b'0') as usize) * 10 + (k3 - b'0') as usize, 19)
                    } else {
                        return None;
                    }
                }
            } else {
                return None;
            }
        };

        let total_len = key_offset + key_len + 2;
        if avail < total_len {
            return Some(ParseResult::Incomplete);
        }
        if unsafe { std::ptr::read_unaligned(buf.add(total_len - 2) as *const u16) } != CRLF {
            return None;
        }

        unsafe {
            if self.parts_raw.capacity() < 2 {
                self.parts_raw.reserve(2);
            }
            let ptr = self.parts_raw.as_mut_ptr();
            std::ptr::write(ptr, (buf.add(8), 3));
            std::ptr::write(ptr.add(1), (buf.add(key_offset), key_len));
            self.parts_raw.set_len(2);
        }
        self.pos += total_len;
        Some(ParseResult::Complete)
    }

    #[inline(always)]
    fn try_fast_set_at(&mut self, buf: *const u8, avail: usize) -> Option<ParseResult> {
        const H2: u32 = u32::from_ne_bytes(*b"$3\r\n");
        const SET_UPPER: u32 = u32::from_ne_bytes(*b"SET\r");
        const SET_LOWER: u32 = u32::from_ne_bytes(*b"set\r");
        const LF_DOLLAR: u16 = u16::from_ne_bytes(*b"\n$");
        const CRLF: u16 = u16::from_ne_bytes(*b"\r\n");

        if avail < 24 {
            return None;
        }
        if unsafe { std::ptr::read_unaligned(buf.add(4) as *const u32) } != H2 {
            return None;
        }
        let w4 = unsafe { std::ptr::read_unaligned(buf.add(8) as *const u32) };
        if w4 != SET_UPPER && w4 != SET_LOWER {
            return None;
        }
        if unsafe { std::ptr::read_unaligned(buf.add(12) as *const u16) } != LF_DOLLAR {
            return None;
        }
        let k1 = unsafe { *buf.add(14) };
        if !k1.is_ascii_digit() {
            return None;
        }
        let (key_len, key_offset) = if unsafe { std::ptr::read_unaligned(buf.add(15) as *const u16) } == CRLF {
            ((k1 - b'0') as usize, 17)
        } else {
            let k2 = unsafe { *buf.add(15) };
            if k2.is_ascii_digit() {
                if unsafe { std::ptr::read_unaligned(buf.add(16) as *const u16) } == CRLF {
                    (((k1 - b'0') as usize) * 10 + (k2 - b'0') as usize, 18)
                } else {
                    let k3 = unsafe { *buf.add(16) };
                    if k3.is_ascii_digit() && unsafe { std::ptr::read_unaligned(buf.add(17) as *const u16) } == CRLF {
                        (((k1 - b'0') as usize) * 100 + ((k2 - b'0') as usize) * 10 + (k3 - b'0') as usize, 19)
                    } else {
                        return None;
                    }
                }
            } else {
                return None;
            }
        };

        let p_val_header = key_offset + key_len + 2;
        if avail < p_val_header + 5 {
            return if avail < p_val_header {
                Some(ParseResult::Incomplete)
            } else {
                None
            };
        }
        if unsafe { std::ptr::read_unaligned(buf.add(p_val_header - 2) as *const u16) } != CRLF {
            return None;
        }
        if unsafe { *buf.add(p_val_header) } != b'$' {
            return None;
        }
        let v1 = unsafe { *buf.add(p_val_header + 1) };
        if !v1.is_ascii_digit() {
            return None;
        }
        let (val_len, val_offset) = if unsafe { std::ptr::read_unaligned(buf.add(p_val_header + 2) as *const u16) } == CRLF {
            ((v1 - b'0') as usize, p_val_header + 4)
        } else {
            let v2 = unsafe { *buf.add(p_val_header + 2) };
            if v2.is_ascii_digit() {
                if unsafe { std::ptr::read_unaligned(buf.add(p_val_header + 3) as *const u16) } == CRLF {
                    (((v1 - b'0') as usize) * 10 + (v2 - b'0') as usize, p_val_header + 5)
                } else {
                    let v3 = unsafe { *buf.add(p_val_header + 3) };
                    if v3.is_ascii_digit() && unsafe { std::ptr::read_unaligned(buf.add(p_val_header + 4) as *const u16) } == CRLF {
                        (((v1 - b'0') as usize) * 100 + ((v2 - b'0') as usize) * 10 + (v3 - b'0') as usize, p_val_header + 6)
                    } else {
                        return None;
                    }
                }
            } else {
                return None;
            }
        };

        let total_len = val_offset + val_len + 2;
        if avail < total_len {
            return Some(ParseResult::Incomplete);
        }
        if unsafe { std::ptr::read_unaligned(buf.add(total_len - 2) as *const u16) } != CRLF {
            return None;
        }

        unsafe {
            if self.parts_raw.capacity() < 3 {
                self.parts_raw.reserve(3);
            }
            let ptr = self.parts_raw.as_mut_ptr();
            std::ptr::write(ptr, (buf.add(8), 3));
            std::ptr::write(ptr.add(1), (buf.add(key_offset), key_len));
            std::ptr::write(ptr.add(2), (buf.add(val_offset), val_len));
            self.parts_raw.set_len(3);
        }
        self.pos += total_len;
        Some(ParseResult::Complete)
    }

    #[inline(always)]
    fn try_fast_ping_at(&mut self, buf: *const u8, avail: usize) -> Option<ParseResult> {
        const H4: u32 = u32::from_ne_bytes(*b"$4\r\n");
        const PING_UPPER: u32 = u32::from_ne_bytes(*b"PING");
        const PING_LOWER: u32 = u32::from_ne_bytes(*b"ping");
        const CRLF: u16 = u16::from_ne_bytes(*b"\r\n");

        if avail < 14 {
            return Some(ParseResult::Incomplete);
        }
        if unsafe { std::ptr::read_unaligned(buf.add(4) as *const u32) } != H4 {
            return None;
        }
        let w4 = unsafe { std::ptr::read_unaligned(buf.add(8) as *const u32) };
        if w4 != PING_UPPER && w4 != PING_LOWER {
            return None;
        }
        if unsafe { std::ptr::read_unaligned(buf.add(12) as *const u16) } != CRLF {
            return None;
        }

        unsafe {
            if self.parts_raw.capacity() < 1 {
                self.parts_raw.reserve(2);
            }
            let ptr = self.parts_raw.as_mut_ptr();
            std::ptr::write(ptr, (buf.add(8), 4));
            self.parts_raw.set_len(1);
        }
        self.pos += 14;
        Some(ParseResult::Complete)
    }

    #[inline(always)]
    fn try_fast_del_at(&mut self, buf: *const u8, avail: usize) -> Option<ParseResult> {
        const H2: u32 = u32::from_ne_bytes(*b"$3\r\n");
        const DEL_UPPER: u32 = u32::from_ne_bytes(*b"DEL\r");
        const DEL_LOWER: u32 = u32::from_ne_bytes(*b"del\r");
        const LF_DOLLAR: u16 = u16::from_ne_bytes(*b"\n$");
        const CRLF: u16 = u16::from_ne_bytes(*b"\r\n");

        if avail < 20 {
            return None;
        }
        if unsafe { std::ptr::read_unaligned(buf.add(4) as *const u32) } != H2 {
            return None;
        }
        let w4 = unsafe { std::ptr::read_unaligned(buf.add(8) as *const u32) };
        if w4 != DEL_UPPER && w4 != DEL_LOWER {
            return None;
        }
        if unsafe { std::ptr::read_unaligned(buf.add(12) as *const u16) } != LF_DOLLAR {
            return None;
        }
        let k1 = unsafe { *buf.add(14) };
        if !k1.is_ascii_digit() {
            return None;
        }
        let (key_len, key_offset) = if unsafe { std::ptr::read_unaligned(buf.add(15) as *const u16) } == CRLF {
            ((k1 - b'0') as usize, 17)
        } else {
            let k2 = unsafe { *buf.add(15) };
            if k2.is_ascii_digit() {
                if unsafe { std::ptr::read_unaligned(buf.add(16) as *const u16) } == CRLF {
                    (((k1 - b'0') as usize) * 10 + (k2 - b'0') as usize, 18)
                } else {
                    let k3 = unsafe { *buf.add(16) };
                    if k3.is_ascii_digit() && unsafe { std::ptr::read_unaligned(buf.add(17) as *const u16) } == CRLF {
                        (((k1 - b'0') as usize) * 100 + ((k2 - b'0') as usize) * 10 + (k3 - b'0') as usize, 19)
                    } else {
                        return None;
                    }
                }
            } else {
                return None;
            }
        };

        let total_len = key_offset + key_len + 2;
        if avail < total_len {
            return Some(ParseResult::Incomplete);
        }
        if unsafe { std::ptr::read_unaligned(buf.add(total_len - 2) as *const u16) } != CRLF {
            return None;
        }

        unsafe {
            if self.parts_raw.capacity() < 2 {
                self.parts_raw.reserve(2);
            }
            let ptr = self.parts_raw.as_mut_ptr();
            std::ptr::write(ptr, (buf.add(8), 3));
            std::ptr::write(ptr.add(1), (buf.add(key_offset), key_len));
            self.parts_raw.set_len(2);
        }
        self.pos += total_len;
        Some(ParseResult::Complete)
    }

    #[inline(always)]
    fn try_fast_hget_at(&mut self, buf: *const u8, avail: usize) -> Option<ParseResult> {
        const H4: u32 = u32::from_ne_bytes(*b"$4\r\n");
        const HGET_UPPER: u32 = u32::from_ne_bytes(*b"HGET");
        const HGET_LOWER: u32 = u32::from_ne_bytes(*b"hget");
        const CRLF: u16 = u16::from_ne_bytes(*b"\r\n");

        if avail < 26 {
            return None;
        }
        if unsafe { std::ptr::read_unaligned(buf.add(4) as *const u32) } != H4 {
            return None;
        }
        let w4 = unsafe { std::ptr::read_unaligned(buf.add(8) as *const u32) };
        if w4 != HGET_UPPER && w4 != HGET_LOWER {
            return None;
        }
        if unsafe { std::ptr::read_unaligned(buf.add(12) as *const u16) } != CRLF {
            return None;
        }
        if unsafe { *buf.add(14) } != b'$' {
            return None;
        }
        let k1 = unsafe { *buf.add(15) };
        if !k1.is_ascii_digit() {
            return None;
        }
        let (key_len, key_offset) = if unsafe { std::ptr::read_unaligned(buf.add(16) as *const u16) } == CRLF {
            ((k1 - b'0') as usize, 18)
        } else {
            let k2 = unsafe { *buf.add(16) };
            if k2.is_ascii_digit() {
                if unsafe { std::ptr::read_unaligned(buf.add(17) as *const u16) } == CRLF {
                    (((k1 - b'0') as usize) * 10 + (k2 - b'0') as usize, 19)
                } else {
                    let k3 = unsafe { *buf.add(17) };
                    if k3.is_ascii_digit() && unsafe { std::ptr::read_unaligned(buf.add(18) as *const u16) } == CRLF {
                        (((k1 - b'0') as usize) * 100 + ((k2 - b'0') as usize) * 10 + (k3 - b'0') as usize, 20)
                    } else {
                        return None;
                    }
                }
            } else {
                return None;
            }
        };

        let p_field_header = key_offset + key_len + 2;
        if avail < p_field_header + 5 {
            return if avail < p_field_header {
                Some(ParseResult::Incomplete)
            } else {
                None
            };
        }
        if unsafe { std::ptr::read_unaligned(buf.add(p_field_header - 2) as *const u16) } != CRLF {
            return None;
        }
        if unsafe { *buf.add(p_field_header) } != b'$' {
            return None;
        }
        let f1 = unsafe { *buf.add(p_field_header + 1) };
        if !f1.is_ascii_digit() {
            return None;
        }
        let (field_len, field_offset) = if unsafe { std::ptr::read_unaligned(buf.add(p_field_header + 2) as *const u16) } == CRLF {
            ((f1 - b'0') as usize, p_field_header + 4)
        } else {
            let f2 = unsafe { *buf.add(p_field_header + 2) };
            if f2.is_ascii_digit() {
                if unsafe { std::ptr::read_unaligned(buf.add(p_field_header + 3) as *const u16) } == CRLF {
                    (((f1 - b'0') as usize) * 10 + (f2 - b'0') as usize, p_field_header + 5)
                } else {
                    let f3 = unsafe { *buf.add(p_field_header + 3) };
                    if f3.is_ascii_digit() && unsafe { std::ptr::read_unaligned(buf.add(p_field_header + 4) as *const u16) } == CRLF {
                        (((f1 - b'0') as usize) * 100 + ((f2 - b'0') as usize) * 10 + (f3 - b'0') as usize, p_field_header + 6)
                    } else {
                        return None;
                    }
                }
            } else {
                return None;
            }
        };

        let total_len = field_offset + field_len + 2;
        if avail < total_len {
            return Some(ParseResult::Incomplete);
        }
        if unsafe { std::ptr::read_unaligned(buf.add(total_len - 2) as *const u16) } != CRLF {
            return None;
        }

        unsafe {
            if self.parts_raw.capacity() < 3 {
                self.parts_raw.reserve(4);
            }
            let ptr = self.parts_raw.as_mut_ptr();
            std::ptr::write(ptr, (buf.add(8), 4));
            std::ptr::write(ptr.add(1), (buf.add(key_offset), key_len));
            std::ptr::write(ptr.add(2), (buf.add(field_offset), field_len));
            self.parts_raw.set_len(3);
        }
        self.pos += total_len;
        Some(ParseResult::Complete)
    }

    #[inline(always)]
    fn try_fast_hset_at(&mut self, buf: *const u8, avail: usize) -> Option<ParseResult> {
        const H4: u32 = u32::from_ne_bytes(*b"$4\r\n");
        const HSET_UPPER: u32 = u32::from_ne_bytes(*b"HSET");
        const HSET_LOWER: u32 = u32::from_ne_bytes(*b"hset");
        const CRLF: u16 = u16::from_ne_bytes(*b"\r\n");

        if avail < 32 {
            return None;
        }
        if unsafe { std::ptr::read_unaligned(buf.add(4) as *const u32) } != H4 {
            return None;
        }
        let w4 = unsafe { std::ptr::read_unaligned(buf.add(8) as *const u32) };
        if w4 != HSET_UPPER && w4 != HSET_LOWER {
            return None;
        }
        if unsafe { std::ptr::read_unaligned(buf.add(12) as *const u16) } != CRLF {
            return None;
        }
        if unsafe { *buf.add(14) } != b'$' {
            return None;
        }
        let k1 = unsafe { *buf.add(15) };
        if !k1.is_ascii_digit() {
            return None;
        }
        let (key_len, key_offset) = if unsafe { std::ptr::read_unaligned(buf.add(16) as *const u16) } == CRLF {
            ((k1 - b'0') as usize, 18)
        } else {
            let k2 = unsafe { *buf.add(16) };
            if k2.is_ascii_digit() {
                if unsafe { std::ptr::read_unaligned(buf.add(17) as *const u16) } == CRLF {
                    (((k1 - b'0') as usize) * 10 + (k2 - b'0') as usize, 19)
                } else {
                    let k3 = unsafe { *buf.add(17) };
                    if k3.is_ascii_digit() && unsafe { std::ptr::read_unaligned(buf.add(18) as *const u16) } == CRLF {
                        (((k1 - b'0') as usize) * 100 + ((k2 - b'0') as usize) * 10 + (k3 - b'0') as usize, 20)
                    } else {
                        return None;
                    }
                }
            } else {
                return None;
            }
        };

        let p_field_header = key_offset + key_len + 2;
        if avail < p_field_header + 5 {
            return if avail < p_field_header {
                Some(ParseResult::Incomplete)
            } else {
                None
            };
        }
        if unsafe { std::ptr::read_unaligned(buf.add(p_field_header - 2) as *const u16) } != CRLF {
            return None;
        }
        if unsafe { *buf.add(p_field_header) } != b'$' {
            return None;
        }
        let f1 = unsafe { *buf.add(p_field_header + 1) };
        if !f1.is_ascii_digit() {
            return None;
        }
        let (field_len, field_offset) = if unsafe { std::ptr::read_unaligned(buf.add(p_field_header + 2) as *const u16) } == CRLF {
            ((f1 - b'0') as usize, p_field_header + 4)
        } else {
            let f2 = unsafe { *buf.add(p_field_header + 2) };
            if f2.is_ascii_digit() {
                if unsafe { std::ptr::read_unaligned(buf.add(p_field_header + 3) as *const u16) } == CRLF {
                    (((f1 - b'0') as usize) * 10 + (f2 - b'0') as usize, p_field_header + 5)
                } else {
                    let f3 = unsafe { *buf.add(p_field_header + 3) };
                    if f3.is_ascii_digit() && unsafe { std::ptr::read_unaligned(buf.add(p_field_header + 4) as *const u16) } == CRLF {
                        (((f1 - b'0') as usize) * 100 + ((f2 - b'0') as usize) * 10 + (f3 - b'0') as usize, p_field_header + 6)
                    } else {
                        return None;
                    }
                }
            } else {
                return None;
            }
        };

        let p_val_header = field_offset + field_len + 2;
        if avail < p_val_header + 5 {
            return if avail < p_val_header {
                Some(ParseResult::Incomplete)
            } else {
                None
            };
        }
        if unsafe { std::ptr::read_unaligned(buf.add(p_val_header - 2) as *const u16) } != CRLF {
            return None;
        }
        if unsafe { *buf.add(p_val_header) } != b'$' {
            return None;
        }
        let v1 = unsafe { *buf.add(p_val_header + 1) };
        if !v1.is_ascii_digit() {
            return None;
        }
        let (val_len, val_offset) = if unsafe { std::ptr::read_unaligned(buf.add(p_val_header + 2) as *const u16) } == CRLF {
            ((v1 - b'0') as usize, p_val_header + 4)
        } else {
            let v2 = unsafe { *buf.add(p_val_header + 2) };
            if v2.is_ascii_digit() {
                if unsafe { std::ptr::read_unaligned(buf.add(p_val_header + 3) as *const u16) } == CRLF {
                    (((v1 - b'0') as usize) * 10 + (v2 - b'0') as usize, p_val_header + 5)
                } else {
                    let v3 = unsafe { *buf.add(p_val_header + 3) };
                    if v3.is_ascii_digit() && unsafe { std::ptr::read_unaligned(buf.add(p_val_header + 4) as *const u16) } == CRLF {
                        (((v1 - b'0') as usize) * 100 + ((v2 - b'0') as usize) * 10 + (v3 - b'0') as usize, p_val_header + 6)
                    } else {
                        return None;
                    }
                }
            } else {
                return None;
            }
        };

        let total_len = val_offset + val_len + 2;
        if avail < total_len {
            return Some(ParseResult::Incomplete);
        }
        if unsafe { std::ptr::read_unaligned(buf.add(total_len - 2) as *const u16) } != CRLF {
            return None;
        }

        unsafe {
            if self.parts_raw.capacity() < 4 {
                self.parts_raw.reserve(4);
            }
            let ptr = self.parts_raw.as_mut_ptr();
            std::ptr::write(ptr, (buf.add(8), 4));
            std::ptr::write(ptr.add(1), (buf.add(key_offset), key_len));
            std::ptr::write(ptr.add(2), (buf.add(field_offset), field_len));
            std::ptr::write(ptr.add(3), (buf.add(val_offset), val_len));
            self.parts_raw.set_len(4);
        }
        self.pos += total_len;
        Some(ParseResult::Complete)
    }

    #[inline(always)]
    fn try_fast_parse_4_at(&mut self, buf: *const u8, avail: usize) -> Option<ParseResult> {
        const CRLF: u16 = u16::from_ne_bytes(*b"\r\n");
        if avail < 24 {
            return None;
        }
        if unsafe { *buf.add(4) } != b'$' {
            return None;
        }
        let c1_digit = unsafe { *buf.add(5) };
        if !c1_digit.is_ascii_digit() || unsafe { std::ptr::read_unaligned(buf.add(6) as *const u16) } != CRLF {
            return None;
        }
        let c1_len = (c1_digit - b'0') as usize;
        let c1_total = 4 + 4 + c1_len + 2;
        if avail < c1_total + 4 {
            return None;
        }
        if unsafe { std::ptr::read_unaligned(buf.add(c1_total - 2) as *const u16) } != CRLF {
            return None;
        }
        let cmd_ptr = unsafe { buf.add(8) };

        // Parse element 2
        let p2 = unsafe { buf.add(c1_total) };
        if unsafe { *p2 } != b'$' {
            return None;
        }
        let mut idx = 1;
        let mut e2_len = 0usize;
        while idx < 6 && (c1_total + idx) < avail {
            let b = unsafe { *p2.add(idx) };
            if b == b'\r' {
                break;
            }
            if !b.is_ascii_digit() {
                return None;
            }
            e2_len = e2_len * 10 + (b - b'0') as usize;
            idx += 1;
        }
        if (c1_total + idx + 1) >= avail || unsafe { std::ptr::read_unaligned(p2.add(idx) as *const u16) } != CRLF {
            return None;
        }
        let e2_data_offset = c1_total + idx + 2;
        let e2_total = e2_data_offset + e2_len + 2;
        if avail < e2_total + 4 {
            return None;
        }
        if unsafe { std::ptr::read_unaligned(buf.add(e2_total - 2) as *const u16) } != CRLF {
            return None;
        }
        let e2_ptr = unsafe { buf.add(e2_data_offset) };

        // Parse element 3
        let p3 = unsafe { buf.add(e2_total) };
        if unsafe { *p3 } != b'$' {
            return None;
        }
        let mut idx3 = 1;
        let mut e3_len = 0usize;
        while idx3 < 7 && (e2_total + idx3) < avail {
            let b = unsafe { *p3.add(idx3) };
            if b == b'\r' {
                break;
            }
            if !b.is_ascii_digit() {
                return None;
            }
            e3_len = e3_len * 10 + (b - b'0') as usize;
            idx3 += 1;
        }
        if (e2_total + idx3 + 1) >= avail || unsafe { std::ptr::read_unaligned(p3.add(idx3) as *const u16) } != CRLF {
            return None;
        }
        let e3_data_offset = e2_total + idx3 + 2;
        let e3_total = e3_data_offset + e3_len + 2;
        if avail < e3_total + 4 {
            return None;
        }
        if unsafe { std::ptr::read_unaligned(buf.add(e3_total - 2) as *const u16) } != CRLF {
            return None;
        }
        let e3_ptr = unsafe { buf.add(e3_data_offset) };

        // Parse element 4
        let p4 = unsafe { buf.add(e3_total) };
        if unsafe { *p4 } != b'$' {
            return None;
        }
        let mut idx4 = 1;
        let mut e4_len = 0usize;
        while idx4 < 7 && (e3_total + idx4) < avail {
            let b = unsafe { *p4.add(idx4) };
            if b == b'\r' {
                break;
            }
            if !b.is_ascii_digit() {
                return None;
            }
            e4_len = e4_len * 10 + (b - b'0') as usize;
            idx4 += 1;
        }
        if (e3_total + idx4 + 1) >= avail || unsafe { std::ptr::read_unaligned(p4.add(idx4) as *const u16) } != CRLF {
            return None;
        }
        let e4_data_offset = e3_total + idx4 + 2;
        let total_cmd_len = e4_data_offset + e4_len + 2;
        if avail < total_cmd_len {
            return Some(ParseResult::Incomplete);
        }
        if unsafe { std::ptr::read_unaligned(buf.add(total_cmd_len - 2) as *const u16) } != CRLF {
            return None;
        }
        let e4_ptr = unsafe { buf.add(e4_data_offset) };

        unsafe {
            if self.parts_raw.capacity() < 4 {
                self.parts_raw.reserve(4);
            }
            let ptr = self.parts_raw.as_mut_ptr();
            std::ptr::write(ptr, (cmd_ptr, c1_len));
            std::ptr::write(ptr.add(1), (e2_ptr, e2_len));
            std::ptr::write(ptr.add(2), (e3_ptr, e3_len));
            std::ptr::write(ptr.add(3), (e4_ptr, e4_len));
            self.parts_raw.set_len(4);
        }
        self.pos += total_cmd_len;
        Some(ParseResult::Complete)
    }

    #[inline(always)]
    fn try_fast_parse_5_at(&mut self, buf: *const u8, avail: usize) -> Option<ParseResult> {
        const CRLF: u16 = u16::from_ne_bytes(*b"\r\n");
        if avail < 30 {
            return None;
        }
        if unsafe { *buf.add(4) } != b'$' {
            return None;
        }
        let c1_digit = unsafe { *buf.add(5) };
        if !c1_digit.is_ascii_digit() || unsafe { std::ptr::read_unaligned(buf.add(6) as *const u16) } != CRLF {
            return None;
        }
        let c1_len = (c1_digit - b'0') as usize;
        let c1_total = 4 + 4 + c1_len + 2;
        if avail < c1_total + 4 {
            return None;
        }
        if unsafe { std::ptr::read_unaligned(buf.add(c1_total - 2) as *const u16) } != CRLF {
            return None;
        }
        let cmd_ptr = unsafe { buf.add(8) };

        // Parse element 2
        let p2 = unsafe { buf.add(c1_total) };
        if unsafe { *p2 } != b'$' {
            return None;
        }
        let mut idx = 1;
        let mut e2_len = 0usize;
        while idx < 6 && (c1_total + idx) < avail {
            let b = unsafe { *p2.add(idx) };
            if b == b'\r' {
                break;
            }
            if !b.is_ascii_digit() {
                return None;
            }
            e2_len = e2_len * 10 + (b - b'0') as usize;
            idx += 1;
        }
        if (c1_total + idx + 1) >= avail || unsafe { std::ptr::read_unaligned(p2.add(idx) as *const u16) } != CRLF {
            return None;
        }
        let e2_data_offset = c1_total + idx + 2;
        let e2_total = e2_data_offset + e2_len + 2;
        if avail < e2_total + 4 {
            return None;
        }
        if unsafe { std::ptr::read_unaligned(buf.add(e2_total - 2) as *const u16) } != CRLF {
            return None;
        }
        let e2_ptr = unsafe { buf.add(e2_data_offset) };

        // Parse element 3
        let p3 = unsafe { buf.add(e2_total) };
        if unsafe { *p3 } != b'$' {
            return None;
        }
        let mut idx3 = 1;
        let mut e3_len = 0usize;
        while idx3 < 7 && (e2_total + idx3) < avail {
            let b = unsafe { *p3.add(idx3) };
            if b == b'\r' {
                break;
            }
            if !b.is_ascii_digit() {
                return None;
            }
            e3_len = e3_len * 10 + (b - b'0') as usize;
            idx3 += 1;
        }
        if (e2_total + idx3 + 1) >= avail || unsafe { std::ptr::read_unaligned(p3.add(idx3) as *const u16) } != CRLF {
            return None;
        }
        let e3_data_offset = e2_total + idx3 + 2;
        let e3_total = e3_data_offset + e3_len + 2;
        if avail < e3_total + 4 {
            return None;
        }
        if unsafe { std::ptr::read_unaligned(buf.add(e3_total - 2) as *const u16) } != CRLF {
            return None;
        }
        let e3_ptr = unsafe { buf.add(e3_data_offset) };

        // Parse element 4
        let p4 = unsafe { buf.add(e3_total) };
        if unsafe { *p4 } != b'$' {
            return None;
        }
        let mut idx4 = 1;
        let mut e4_len = 0usize;
        while idx4 < 7 && (e3_total + idx4) < avail {
            let b = unsafe { *p4.add(idx4) };
            if b == b'\r' {
                break;
            }
            if !b.is_ascii_digit() {
                return None;
            }
            e4_len = e4_len * 10 + (b - b'0') as usize;
            idx4 += 1;
        }
        if (e3_total + idx4 + 1) >= avail || unsafe { std::ptr::read_unaligned(p4.add(idx4) as *const u16) } != CRLF {
            return None;
        }
        let e4_data_offset = e3_total + idx4 + 2;
        let e4_total = e4_data_offset + e4_len + 2;
        if avail < e4_total + 4 {
            return None;
        }
        if unsafe { std::ptr::read_unaligned(buf.add(e4_total - 2) as *const u16) } != CRLF {
            return None;
        }
        let e4_ptr = unsafe { buf.add(e4_data_offset) };

        // Parse element 5
        let p5 = unsafe { buf.add(e4_total) };
        if unsafe { *p5 } != b'$' {
            return None;
        }
        let mut idx5 = 1;
        let mut e5_len = 0usize;
        while idx5 < 7 && (e4_total + idx5) < avail {
            let b = unsafe { *p5.add(idx5) };
            if b == b'\r' {
                break;
            }
            if !b.is_ascii_digit() {
                return None;
            }
            e5_len = e5_len * 10 + (b - b'0') as usize;
            idx5 += 1;
        }
        if (e4_total + idx5 + 1) >= avail || unsafe { std::ptr::read_unaligned(p5.add(idx5) as *const u16) } != CRLF {
            return None;
        }
        let e5_data_offset = e4_total + idx5 + 2;
        let total_cmd_len = e5_data_offset + e5_len + 2;
        if avail < total_cmd_len {
            return Some(ParseResult::Incomplete);
        }
        if unsafe { std::ptr::read_unaligned(buf.add(total_cmd_len - 2) as *const u16) } != CRLF {
            return None;
        }
        let e5_ptr = unsafe { buf.add(e5_data_offset) };

        unsafe {
            if self.parts_raw.capacity() < 5 {
                self.parts_raw.reserve(5);
            }
            let ptr = self.parts_raw.as_mut_ptr();
            std::ptr::write(ptr, (cmd_ptr, c1_len));
            std::ptr::write(ptr.add(1), (e2_ptr, e2_len));
            std::ptr::write(ptr.add(2), (e3_ptr, e3_len));
            std::ptr::write(ptr.add(3), (e4_ptr, e4_len));
            std::ptr::write(ptr.add(4), (e5_ptr, e5_len));
            self.parts_raw.set_len(5);
        }
        self.pos += total_cmd_len;
        Some(ParseResult::Complete)
    }

    #[inline(always)]
    fn try_fast_parse_2_at(&mut self, buf: *const u8, avail: usize) -> Option<ParseResult> {
        const CRLF: u16 = u16::from_ne_bytes(*b"\r\n");
        if avail < 16 {
            return None;
        }
        if unsafe { *buf.add(4) } != b'$' {
            return None;
        }
        let c1_digit = unsafe { *buf.add(5) };
        if !c1_digit.is_ascii_digit() || unsafe { std::ptr::read_unaligned(buf.add(6) as *const u16) } != CRLF {
            return None;
        }
        let c1_len = (c1_digit - b'0') as usize;
        let c1_total = 4 + 4 + c1_len + 2;
        if avail < c1_total + 4 {
            return None;
        }
        if unsafe { std::ptr::read_unaligned(buf.add(c1_total - 2) as *const u16) } != CRLF {
            return None;
        }
        let cmd_ptr = unsafe { buf.add(8) };

        let p2 = unsafe { buf.add(c1_total) };
        if unsafe { *p2 } != b'$' {
            return None;
        }
        let k1 = unsafe { *p2.add(1) };
        if !k1.is_ascii_digit() {
            return None;
        }
        let (key_len, idx) = if unsafe { std::ptr::read_unaligned(p2.add(2) as *const u16) } == CRLF {
            ((k1 - b'0') as usize, 2)
        } else {
            let k2 = unsafe { *p2.add(2) };
            if k2.is_ascii_digit() && unsafe { std::ptr::read_unaligned(p2.add(3) as *const u16) } == CRLF {
                (((k1 - b'0') as usize) * 10 + (k2 - b'0') as usize, 3)
            } else {
                let mut i = 1;
                let mut kl = 0usize;
                while i < 6 && (c1_total + i) < avail {
                    let b = unsafe { *p2.add(i) };
                    if b == b'\r' {
                        break;
                    }
                    if !b.is_ascii_digit() {
                        return None;
                    }
                    kl = kl * 10 + (b - b'0') as usize;
                    i += 1;
                }
                if (c1_total + i + 1) >= avail || unsafe { *p2.add(i) } != b'\r' || unsafe { *p2.add(i + 1) } != b'\n' {
                    return None;
                }
                (kl, i)
            }
        };

        let key_data_offset = c1_total + idx + 2;
        let total_cmd_len = key_data_offset + key_len + 2;
        if avail < total_cmd_len {
            return Some(ParseResult::Incomplete);
        }
        if unsafe { std::ptr::read_unaligned(buf.add(total_cmd_len - 2) as *const u16) } != CRLF {
            return None;
        }
        let key_ptr = unsafe { buf.add(key_data_offset) };

        unsafe {
            if self.parts_raw.capacity() < 2 {
                self.parts_raw.reserve(2);
            }
            let ptr = self.parts_raw.as_mut_ptr();
            std::ptr::write(ptr, (cmd_ptr, c1_len));
            std::ptr::write(ptr.add(1), (key_ptr, key_len));
            self.parts_raw.set_len(2);
        }
        self.pos += total_cmd_len;
        Some(ParseResult::Complete)
    }

    #[inline(always)]
    fn try_fast_parse_3_at(&mut self, buf: *const u8, avail: usize) -> Option<ParseResult> {
        const CRLF: u16 = u16::from_ne_bytes(*b"\r\n");
        if avail < 20 {
            return None;
        }
        if unsafe { *buf.add(4) } != b'$' {
            return None;
        }
        let c1_digit = unsafe { *buf.add(5) };
        if !c1_digit.is_ascii_digit() || unsafe { std::ptr::read_unaligned(buf.add(6) as *const u16) } != CRLF {
            return None;
        }
        let c1_len = (c1_digit - b'0') as usize;
        let c1_total = 4 + 4 + c1_len + 2;
        if avail < c1_total + 4 {
            return None;
        }
        if unsafe { std::ptr::read_unaligned(buf.add(c1_total - 2) as *const u16) } != CRLF {
            return None;
        }
        let cmd_ptr = unsafe { buf.add(8) };

        let p2 = unsafe { buf.add(c1_total) };
        if unsafe { *p2 } != b'$' {
            return None;
        }
        let mut idx = 1;
        let mut key_len = 0usize;
        while idx < 6 && (c1_total + idx) < avail {
            let b = unsafe { *p2.add(idx) };
            if b == b'\r' {
                break;
            }
            if !b.is_ascii_digit() {
                return None;
            }
            key_len = key_len * 10 + (b - b'0') as usize;
            idx += 1;
        }
        if (c1_total + idx + 1) >= avail || unsafe { *p2.add(idx) } != b'\r' || unsafe { *p2.add(idx + 1) } != b'\n' {
            return None;
        }
        let key_data_offset = c1_total + idx + 2;
        let e2_total = key_data_offset + key_len + 2;
        if avail < e2_total + 4 {
            return None;
        }
        if unsafe { std::ptr::read_unaligned(buf.add(e2_total - 2) as *const u16) } != CRLF {
            return None;
        }
        let key_ptr = unsafe { buf.add(key_data_offset) };

        let p3 = unsafe { buf.add(e2_total) };
        if unsafe { *p3 } != b'$' {
            return None;
        }
        let mut idx3 = 1;
        let mut val_len = 0usize;
        while idx3 < 7 && (e2_total + idx3) < avail {
            let b = unsafe { *p3.add(idx3) };
            if b == b'\r' {
                break;
            }
            if !b.is_ascii_digit() {
                return None;
            }
            val_len = val_len * 10 + (b - b'0') as usize;
            idx3 += 1;
        }
        if (e2_total + idx3 + 1) >= avail || unsafe { *p3.add(idx3) } != b'\r' || unsafe { *p3.add(idx3 + 1) } != b'\n' {
            return None;
        }
        let val_data_offset = e2_total + idx3 + 2;
        let total_cmd_len = val_data_offset + val_len + 2;
        if avail < total_cmd_len {
            return Some(ParseResult::Incomplete);
        }
        if unsafe { std::ptr::read_unaligned(buf.add(total_cmd_len - 2) as *const u16) } != CRLF {
            return None;
        }
        let val_ptr = unsafe { buf.add(val_data_offset) };

        unsafe {
            if self.parts_raw.capacity() < 3 {
                self.parts_raw.reserve(4);
            }
            let ptr = self.parts_raw.as_mut_ptr();
            std::ptr::write(ptr, (cmd_ptr, c1_len));
            std::ptr::write(ptr.add(1), (key_ptr, key_len));
            std::ptr::write(ptr.add(2), (val_ptr, val_len));
            self.parts_raw.set_len(3);
        }
        self.pos += total_cmd_len;
        Some(ParseResult::Complete)
    }

    pub fn parse_one(&mut self) -> ParseResult {
        self.parts_raw.clear();
        if self.partial.is_some() {
            return self.resume();
        }

        let avail = self.filled - self.pos;
        if avail >= 14 {
            let buf = unsafe { self.rbuf.as_ptr().add(self.pos) };
            let tag = unsafe { std::ptr::read_unaligned(buf as *const u32) };
            const T1: u32 = u32::from_ne_bytes(*b"*1\r\n");
            const T2: u32 = u32::from_ne_bytes(*b"*2\r\n");
            const T3: u32 = u32::from_ne_bytes(*b"*3\r\n");
            const T4: u32 = u32::from_ne_bytes(*b"*4\r\n");
            const T5: u32 = u32::from_ne_bytes(*b"*5\r\n");

            if tag == T2 {
                if let Some(res) = self.try_fast_get_at(buf, avail) {
                    return res;
                }
                if let Some(res) = self.try_fast_del_at(buf, avail) {
                    return res;
                }
                if let Some(res) = self.try_fast_parse_2_at(buf, avail) {
                    return res;
                }
            } else if tag == T3 {
                if let Some(res) = self.try_fast_set_at(buf, avail) {
                    return res;
                }
                if let Some(res) = self.try_fast_hget_at(buf, avail) {
                    return res;
                }
                if let Some(res) = self.try_fast_parse_3_at(buf, avail) {
                    return res;
                }
            } else if tag == T4 {
                if let Some(res) = self.try_fast_hset_at(buf, avail) {
                    return res;
                }
                if let Some(res) = self.try_fast_parse_4_at(buf, avail) {
                    return res;
                }
            } else if tag == T5 {
                if let Some(res) = self.try_fast_parse_5_at(buf, avail) {
                    return res;
                }
            } else if tag == T1 {
                if let Some(res) = self.try_fast_ping_at(buf, avail) {
                    return res;
                }
            }
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

        if self.parts_raw.capacity() < count {
            self.parts_raw.reserve(count.min(MAX_PREALLOC_PARTS));
        }

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
        let ptr = unsafe { self.rbuf.as_ptr().add(self.pos) };
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
            // SAFETY: offsets come from live elements in rbuf[start..self.pos]
            // that remain valid until the next read_buf.
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
        if self.pos == self.filled && self.partial.is_none() {
            self.parts_raw.clear();
            self.filled = 0;
            self.pos = 0;
            if self.rbuf.len() > 16 * 1024 {
                self.rbuf.truncate(Self::IDLE_READ_BUFFER);
                self.rbuf.shrink_to(Self::IDLE_READ_BUFFER);
            }
        }
        if self.wbuf.is_empty() && self.wbuf.capacity() > Self::IDLE_READ_BUFFER {
            self.wbuf = Vec::new();
        }
    }

    #[inline(always)]
    fn scan_line(&mut self) -> Option<(usize, usize)> {
        let start = self.pos;
        let slice = self.rbuf.get(start..self.filled)?;
        let len = slice.len();
        if len == 0 {
            return None;
        }
        // Direct fast scan for short lines (< 8 bytes)
        let check_len = len.min(8);
        let mut rel = None;
        for i in 0..check_len {
            if unsafe { *slice.get_unchecked(i) } == b'\n' {
                rel = Some(i);
                break;
            }
        }
        let rel = match rel {
            Some(r) => r,
            None => {
                if len > 8 {
                    memchr::memchr(b'\n', &slice[8..])? + 8
                } else {
                    return None;
                }
            }
        };
        let nl = start + rel;
        self.pos = nl + 1;
        let end = if nl > start && unsafe { *self.rbuf.get_unchecked(nl - 1) } == b'\r' {
            nl - 1
        } else {
            nl
        };
        Some((start, end))
    }
}

#[inline(always)]
fn parse_usize(s: &[u8], max: usize) -> Option<usize> {
    if s.is_empty() {
        return None;
    }
    if s.len() <= 18 {
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

    /// Resumes multi-read commands from saved element index even when
    /// read_buf compacts or reallocates.
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

    #[test]
    fn fast_parser_parses_5_part_command() {
        let mut parser = RespParser::new();
        feed(&mut parser, b"*5\r\n$4\r\nXADD\r\n$6\r\nstream\r\n$1\r\n*\r\n$1\r\nf\r\n$3\r\n100\r\n");
        assert!(matches!(parser.parse_one(), ParseResult::Complete));
        assert_eq!(parts_of(&parser), vec!["XADD", "stream", "*", "f", "100"]);
    }
}

