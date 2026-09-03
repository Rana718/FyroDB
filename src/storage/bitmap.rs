use crate::storage::store::Store;
use crate::storage::value::{FyroDB, SmallStr, StoreValue};

impl Store {
    pub fn setbit(&self, key: &str, offset: u64, value: bool) -> Result<u8, &'static str> {
        let byte_idx = (offset / 8) as usize;
        let bit_idx = 7 - (offset % 8) as u8;

        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                let mut bytes = vec![0u8; byte_idx + 1];
                if value {
                    bytes[byte_idx] |= 1 << bit_idx;
                }
                val.value = FyroDB::String(SmallStr::from_bytes(bytes));
                val.expires_ms = 0;
                return Ok(0u8);
            }
            match val.value.as_string_mut() {
                Some(s) => {
                    s.ensure_len(byte_idx + 1);
                    let bytes = s.as_bytes_mut();
                    let old = (bytes[byte_idx] >> bit_idx) & 1;
                    if value {
                        bytes[byte_idx] |= 1 << bit_idx;
                    } else {
                        bytes[byte_idx] &= !(1 << bit_idx);
                    }
                    Ok(old)
                }
                None => Err("WRONGTYPE"),
            }
        });

        match result {
            Some(r) => r,
            None => {
                let mut bytes = vec![0u8; byte_idx + 1];
                if value {
                    bytes[byte_idx] |= 1 << bit_idx;
                }
                self.data.insert_str(
                    key,
                    StoreValue::string(unsafe { String::from_utf8_unchecked(bytes) }),
                );
                Ok(0)
            }
        }
    }

    pub fn getbit(&self, key: &str, offset: u64) -> Result<u8, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(0),
            Some(e) if e.is_expired() => Ok(0),
            Some(e) => match e.value.as_string() {
                Some(s) => {
                    let byte_idx = (offset / 8) as usize;
                    let bit_idx = 7 - (offset % 8) as u8;
                    let bytes = s.as_bytes();
                    if byte_idx >= bytes.len() {
                        Ok(0)
                    } else {
                        Ok((bytes[byte_idx] >> bit_idx) & 1)
                    }
                }
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn bitcount(
        &self,
        key: &str,
        start: i64,
        end: i64,
        use_bit: bool,
    ) -> Result<usize, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(0),
            Some(e) if e.is_expired() => Ok(0),
            Some(e) => match e.value.as_string() {
                Some(s) => {
                    let bytes = s.as_bytes();
                    if bytes.is_empty() {
                        return Ok(0);
                    }
                    if use_bit {
                        let bit_len = bytes.len() * 8;
                        let s_idx = normalize_bit_index(start, bit_len);
                        let e_idx = normalize_bit_index(end, bit_len);
                        if s_idx > e_idx {
                            return Ok(0);
                        }
                        let mut count = 0usize;
                        for i in s_idx..=e_idx {
                            let byte_idx = i / 8;
                            let bit_idx = 7 - (i % 8);
                            if byte_idx < bytes.len() && (bytes[byte_idx] >> bit_idx) & 1 == 1 {
                                count += 1;
                            }
                        }
                        Ok(count)
                    } else {
                        let len = bytes.len() as i64;
                        let s_idx = if start < 0 {
                            (len + start).max(0)
                        } else {
                            start.min(len)
                        } as usize;
                        let e_idx = if end < 0 {
                            (len + end).max(0)
                        } else {
                            end.min(len - 1)
                        } as usize;
                        if s_idx > e_idx {
                            return Ok(0);
                        }
                        let count = bytes[s_idx..=e_idx]
                            .iter()
                            .map(|b| b.count_ones() as usize)
                            .sum();
                        Ok(count)
                    }
                }
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn bitpos(
        &self,
        key: &str,
        bit: u8,
        start: i64,
        end: i64,
        has_end: bool,
        use_bit: bool,
    ) -> Result<i64, &'static str> {
        match self.data.get_ref(key) {
            None => {
                if bit == 0 {
                    Ok(0)
                } else {
                    Ok(-1)
                }
            }
            Some(e) if e.is_expired() => {
                if bit == 0 {
                    Ok(0)
                } else {
                    Ok(-1)
                }
            }
            Some(e) => match e.value.as_string() {
                Some(s) => {
                    let bytes = s.as_bytes();
                    if bytes.is_empty() {
                        return Ok(if bit == 0 { 0 } else { -1 });
                    }
                    if use_bit {
                        let bit_len = bytes.len() * 8;
                        let s_idx = normalize_bit_index(start, bit_len);
                        let e_idx = normalize_bit_index(end, bit_len);
                        if s_idx > e_idx {
                            return Ok(-1);
                        }
                        for i in s_idx..=e_idx {
                            let byte_idx = i / 8;
                            let bit_idx = 7 - (i % 8);
                            let val = if byte_idx < bytes.len() {
                                (bytes[byte_idx] >> bit_idx) & 1
                            } else {
                                0
                            };
                            if val == bit {
                                return Ok(i as i64);
                            }
                        }
                        Ok(-1)
                    } else {
                        let len = bytes.len() as i64;
                        let s_idx = if start < 0 {
                            (len + start).max(0)
                        } else {
                            start.min(len)
                        } as usize;
                        let e_idx = if end < 0 {
                            (len + end).max(0) as usize
                        } else if has_end {
                            end.min(len - 1) as usize
                        } else {
                            bytes.len() - 1
                        };
                        if s_idx > e_idx {
                            return Ok(-1);
                        }
                        for (byte_idx, &b) in bytes.iter().enumerate().take(e_idx + 1).skip(s_idx) {
                            for bit_idx in (0..8).rev() {
                                let val = (b >> bit_idx) & 1;
                                if val == bit {
                                    let pos = byte_idx * 8 + (7 - bit_idx);
                                    return Ok(pos as i64);
                                }
                            }
                        }
                        if bit == 0 && !has_end {
                            Ok((bytes.len() * 8) as i64)
                        } else {
                            Ok(-1)
                        }
                    }
                }
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn bitop(&self, op: BitOp, dest: &str, keys: &[&str]) -> Result<usize, &'static str> {
        let mut max_len = 0usize;
        let mut buffers: Vec<Vec<u8>> = Vec::with_capacity(keys.len());

        for &k in keys {
            match self.data.get_ref(k) {
                None => buffers.push(vec![]),
                Some(e) if e.is_expired() => buffers.push(vec![]),
                Some(e) => match e.value.as_string() {
                    Some(s) => {
                        let b = s.as_bytes().to_vec();
                        max_len = max_len.max(b.len());
                        buffers.push(b);
                    }
                    None => return Err("WRONGTYPE"),
                },
            }
        }

        if buffers.is_empty() {
            self.data
                .insert(dest.to_string(), StoreValue::string(String::new()));
            return Ok(0);
        }

        let mut result = vec![0u8; max_len];

        match op {
            BitOp::And => {
                result = vec![0xFF; max_len];
                for buf in &buffers {
                    for i in 0..max_len {
                        let b = if i < buf.len() { buf[i] } else { 0 };
                        result[i] &= b;
                    }
                }
            }
            BitOp::Or => {
                for buf in &buffers {
                    for i in 0..buf.len() {
                        result[i] |= buf[i];
                    }
                }
            }
            BitOp::Xor => {
                for buf in &buffers {
                    for i in 0..buf.len() {
                        result[i] ^= buf[i];
                    }
                }
            }
            BitOp::Not => {
                if buffers.len() != 1 {
                    return Err("BITOP NOT requires one source key");
                }
                for i in 0..max_len {
                    result[i] = if i < buffers[0].len() {
                        !buffers[0][i]
                    } else {
                        0xFF
                    };
                }
            }
        }

        let len = result.len();
        self.data.insert(
            dest.to_string(),
            StoreValue::string(unsafe { String::from_utf8_unchecked(result) }),
        );
        Ok(len)
    }
}

#[derive(Clone, Copy)]
pub enum BitOp {
    And,
    Or,
    Xor,
    Not,
}

#[inline]
fn normalize_bit_index(idx: i64, bit_len: usize) -> usize {
    if idx < 0 {
        (bit_len as i64 + idx).max(0) as usize
    } else {
        (idx as usize).min(bit_len.saturating_sub(1))
    }
}
