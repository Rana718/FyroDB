use crate::storage::{
    store::Store,
    value::{SmallStr, StoreValue},
};
use crate::utils::util::format_float;
use customhash::Full;

const MAX_STRING_BYTES: usize = 512 * 1024 * 1024;

impl Store {
    pub fn set(&self, key: String, value: StoreValue) {
        if value.expires_ms != 0 {
            self.add_ttl();
        }
        self.data.insert(key, value);
    }

    pub fn try_set_value(&self, key: String, value: StoreValue) -> Result<(), Full> {
        if value.expires_ms != 0 {
            self.add_ttl();
        }
        self.data.try_insert(key, value)?;
        Ok(())
    }

    #[inline]
    pub fn set_string(&self, key: &str, value: &str, expires_ms: u64) {
        let store_val = StoreValue {
            value: crate::storage::value::FyroDB::String(SmallStr::new(value)),
            expires_ms,
        };
        self.data.set_str(key, store_val);
        if expires_ms != 0 {
            self.add_ttl();
        }
    }

    #[inline]
    pub fn try_set_string(&self, key: &str, value: &str, expires_ms: u64) -> Result<(), Full> {
        let store_val = StoreValue {
            value: crate::storage::value::FyroDB::String(SmallStr::new(value)),
            expires_ms,
        };
        self.data.try_set_str(key, store_val)?;
        if expires_ms != 0 {
            self.add_ttl();
        }
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let (h, idx) = self.data.locate_key(key);
        let result = self.data.with_entry(key, h, idx, |val| {
            if val.is_expired() {
                return Err(());
            }
            Ok(val.value.as_string().map(|s| s.to_string()))
        })?;
        match result {
            Err(()) => {
                self.data.remove(key);
                None
            }
            Ok(v) => v,
        }
    }

    #[inline]
    pub fn get_to_buf(&self, key: &str, out: &mut Vec<u8>) -> bool {
        let (h, idx) = self.data.locate_key(key);
        let found = self.data.with_entry(key, h, idx, |val| {
            if val.is_expired() {
                return Err(());
            }
            match val.value.as_string() {
                Some(s) => {
                    crate::utils::resp::write_bulk(out, s);
                    Ok(true)
                }
                None => Ok(false),
            }
        });
        match found {
            Some(Ok(true)) => true,
            Some(Err(())) => {
                self.data.remove(key);
                false
            }
            _ => false,
        }
    }

    pub fn getdel(&self, key: &str) -> Option<String> {
        let entry = self.data.remove(key)?;
        if entry.is_expired() {
            return None;
        }
        entry.value.as_string().map(|s| s.to_string())
    }

    pub fn getset(&self, key: &str, new_value: &str) -> Option<String> {
        let result = self.data.update_with(key, |val| {
            let old = if val.is_expired() {
                None
            } else {
                val.value.as_string().map(|s| s.to_string())
            };
            val.value = crate::storage::value::FyroDB::String(SmallStr::new(new_value));
            val.expires_ms = 0;
            old
        });
        match result {
            Some(old) => old,
            None => {
                self.data.insert_str(
                    key,
                    StoreValue::string(new_value.to_string()),
                );
                None
            }
        }
    }

    pub fn getex_ms(&self, key: &str, expires_ms: u64) -> Option<String> {
        let (result, ttl_change) = self.data.update_with(key, |val| {
            if val.is_expired() {
                return (None, 0i8);
            }
            let Some(s) = val.value.as_string().map(|s| s.to_string()) else {
                return (None, 0);
            };
            let ttl_change = match (val.expires_ms == 0, expires_ms == 0) {
                (true, false) => 1,
                (false, true) => -1,
                _ => 0,
            };
            val.expires_ms = expires_ms;
            (Some(s), ttl_change)
        })?;
        match ttl_change {
            1 => self.add_ttl(),
            -1 => self.sub_ttl(),
            _ => {}
        }
        result
    }

    pub fn setnx(&self, key: String, value: String) -> bool {
        if let Some(replaced) = self.data.try_update(&key, |current| {
            if current.is_expired() {
                Some((StoreValue::string(value.clone()), true))
            } else {
                Some((current.clone(), false))
            }
        }) {
            return replaced;
        }
        self.data.insert_if_absent(key, StoreValue::string(value))
    }

    pub fn append(&self, key: &str, suffix: &str) -> Result<usize, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                val.value = crate::storage::value::FyroDB::String(SmallStr::from_string(
                    suffix.to_string(),
                ));
                val.expires_ms = 0;
                return Ok(suffix.len());
            }
            match val.value.as_string_mut() {
                Some(s) => {
                    s.push_str(suffix);
                    Ok(s.len())
                }
                None => Err("WRONGTYPE"),
            }
        });

        match result {
            Some(r) => r,
            None => {
                let len = suffix.len();
                self.data
                    .insert_str(key, StoreValue::string(suffix.to_string()));
                Ok(len)
            }
        }
    }

    pub fn strlen(&self, key: &str) -> Result<usize, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(0),
            Some(e) if e.is_expired() => Ok(0),
            Some(e) => match e.value.as_string() {
                Some(s) => Ok(s.len()),
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn getrange(&self, key: &str, start: i64, end: i64) -> String {
        let entry = match self.data.get_ref(key) {
            Some(e) if !e.is_expired() => e,
            _ => return String::new(),
        };
        let s = match entry.value.as_string() {
            Some(s) => s,
            None => return String::new(),
        };

        let len = s.len() as i64;
        if len == 0 {
            return String::new();
        }

        let mut start = if start < 0 {
            (len + start).max(0)
        } else {
            start.min(len)
        } as usize;
        let mut end = if end < 0 {
            (len + end).max(0)
        } else {
            end.min(len - 1)
        } as usize;

        if start > end {
            return String::new();
        }

        while start > 0 && !s.is_char_boundary(start) {
            start -= 1;
        }
        end += 1;
        while end < s.len() && !s.is_char_boundary(end) {
            end += 1;
        }
        s[start..end].to_string()
    }

    pub fn setrange(&self, key: &str, offset: usize, value: &str) -> Result<usize, &'static str> {
        let Some(needed) = offset.checked_add(value.len()) else {
            return Err("offset is out of range");
        };
        if needed > MAX_STRING_BYTES {
            return Err("string exceeds maximum allowed size");
        }

        let result = self
            .data
            .update_with(key, |val| match val.value.as_string_mut() {
                Some(s) => {
                    let mut bytes = std::mem::take(s).into_bytes();
                    if bytes.len() < needed {
                        bytes.resize(needed, 0u8);
                    }
                    bytes[offset..offset + value.len()].copy_from_slice(value.as_bytes());
                    match String::from_utf8(bytes) {
                        Ok(new_s) => {
                            let len = new_s.len();
                            *s = SmallStr::from_string(new_s);
                            Ok(len)
                        }
                        Err(e) => {
                            *s = SmallStr::from_string(unsafe {
                                String::from_utf8_unchecked(e.into_bytes())
                            });
                            Err("result would not be valid UTF-8")
                        }
                    }
                }
                None => Err("WRONGTYPE"),
            });

        match result {
            Some(r) => r,
            None => {
                let mut bytes = vec![0u8; needed];
                bytes[offset..].copy_from_slice(value.as_bytes());
                let new_s = match String::from_utf8(bytes) {
                    Ok(s) => s,
                    Err(_) => return Err("result would not be valid UTF-8"),
                };
                let len = new_s.len();
                if self
                    .data
                    .insert_if_absent(key.to_string(), StoreValue::string(new_s))
                {
                    Ok(len)
                } else {
                    self.setrange(key, offset, value)
                }
            }
        }
    }

    fn int_op(&self, key: &str, delta: i64) -> Result<i64, &'static str> {
        loop {
            let result = self
                .data
                .update_with(key, |val| match val.value.as_string() {
                    Some(s) => {
                        let n = match s.parse::<i64>() {
                            Ok(n) => n,
                            Err(_) => return Err("value is not an integer or out of range"),
                        };
                        let Some(new) = n.checked_add(delta) else {
                            return Err("increment or decrement would overflow");
                        };
                        val.value = crate::storage::value::FyroDB::String(SmallStr::from_int(new));
                        Ok(new)
                    }
                    None => Err("WRONGTYPE"),
                });
            if let Some(r) = result {
                return r;
            }
            // Absent: lock-free create. Losing the insert race just means the
            // key now exists, so the loop retries the in-place update.
            if self.data.insert_if_absent_str(
                key,
                StoreValue::string_small(SmallStr::from_int(delta)),
            ) {
                return Ok(delta);
            }
        }
    }

    pub fn incr(&self, key: &str) -> Result<i64, &'static str> {
        self.int_op(key, 1)
    }
    pub fn decr(&self, key: &str) -> Result<i64, &'static str> {
        self.int_op(key, -1)
    }
    pub fn incrby(&self, key: &str, by: i64) -> Result<i64, &'static str> {
        self.int_op(key, by)
    }
    pub fn decrby(&self, key: &str, by: i64) -> Result<i64, &'static str> {
        let delta = by
            .checked_neg()
            .ok_or("increment or decrement would overflow")?;
        self.int_op(key, delta)
    }

    pub fn incrbyfloat(&self, key: &str, by: f64) -> Result<f64, &'static str> {
        let result = self
            .data
            .update_with(key, |val| match val.value.as_string() {
                Some(s) => {
                    let n = match s.parse::<f64>() {
                        Ok(n) => n,
                        Err(_) => return Err("value is not a valid float"),
                    };
                    let new = n + by;
                    val.value = crate::storage::value::FyroDB::String(SmallStr::from_string(
                        format_float(new),
                    ));
                    Ok(new)
                }
                None => Err("WRONGTYPE"),
            });

        match result {
            Some(r) => r,
            None => {
                self.data
                    .insert_str(key, StoreValue::string(format_float(by)));
                Ok(by)
            }
        }
    }
}

impl Store {
    pub fn lcs(&self, key1: &str, key2: &str) -> Result<String, &'static str> {
        let s1 = match self.data.get_ref(key1) {
            None => String::new(),
            Some(e) if e.is_expired() => String::new(),
            Some(e) => match e.value.as_string() {
                Some(s) => s.to_string(),
                None => return Err("WRONGTYPE"),
            },
        };
        let s2 = match self.data.get_ref(key2) {
            None => String::new(),
            Some(e) if e.is_expired() => String::new(),
            Some(e) => match e.value.as_string() {
                Some(s) => s.to_string(),
                None => return Err("WRONGTYPE"),
            },
        };

        let b1 = s1.as_bytes();
        let b2 = s2.as_bytes();
        let m = b1.len();
        let n = b2.len();

        if m == 0 || n == 0 {
            return Ok(String::new());
        }

        let mut prev = vec![0u16; n + 1];
        let mut curr = vec![0u16; n + 1];

        for i in 1..=m {
            for j in 1..=n {
                if b1[i - 1] == b2[j - 1] {
                    curr[j] = prev[j - 1] + 1;
                } else {
                    curr[j] = prev[j].max(curr[j - 1]);
                }
            }
            std::mem::swap(&mut prev, &mut curr);
            curr.fill(0);
        }

        let lcs_len = prev[n] as usize;
        let mut result = Vec::with_capacity(lcs_len);

        let mut dp = vec![vec![0u16; n + 1]; m + 1];
        for i in 1..=m {
            for j in 1..=n {
                if b1[i - 1] == b2[j - 1] {
                    dp[i][j] = dp[i - 1][j - 1] + 1;
                } else {
                    dp[i][j] = dp[i - 1][j].max(dp[i][j - 1]);
                }
            }
        }

        let mut i = m;
        let mut j = n;
        while i > 0 && j > 0 {
            if b1[i - 1] == b2[j - 1] {
                result.push(b1[i - 1]);
                i -= 1;
                j -= 1;
            } else if dp[i - 1][j] > dp[i][j - 1] {
                i -= 1;
            } else {
                j -= 1;
            }
        }
        result.reverse();

        Ok(String::from_utf8(result).unwrap_or_default())
    }

    pub fn lcs_len(&self, key1: &str, key2: &str) -> Result<usize, &'static str> {
        let s = self.lcs(key1, key2)?;
        Ok(s.len())
    }
}
