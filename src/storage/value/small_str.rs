pub(crate) const SMALL_STR_CAP: usize = 15;

#[repr(C)]
pub struct SmallStr {
    data: [u8; SMALL_STR_CAP],
    len: u8,
}

impl SmallStr {
    #[inline]
    fn store_heap_len(data: &mut [u8; SMALL_STR_CAP], len: usize) {
        let lb = (len as u64).to_ne_bytes();
        data[8..15].copy_from_slice(&lb[..7]);
    }

    #[inline(always)]
    fn read_heap_len(data: &[u8; SMALL_STR_CAP]) -> usize {
        let mut lb = [0u8; 8];
        lb[..7].copy_from_slice(&data[8..15]);
        u64::from_ne_bytes(lb) as usize
    }

    /// Formats an integer into the inline buffer. i64 never exceeds 20 ASCII
    /// digits, so this allocates only when the digits exceed SMALL_STR_CAP.
    #[inline]
    pub fn from_int(mut n: i64) -> Self {
        let mut buf = [0u8; 20];
        let neg = n < 0;
        if neg {
            // i64::MIN handled without overflow: negate digit-wise below.
            let mut m = (n as i128).unsigned_abs();
            let mut i = buf.len();
            while m >= 10 {
                i -= 1;
                buf[i] = b'0' + (m % 10) as u8;
                m /= 10;
            }
            i -= 1;
            buf[i] = b'0' + m as u8;
            i -= 1;
            buf[i] = b'-';
            return Self::new(unsafe { std::str::from_utf8_unchecked(&buf[i..]) });
        }
        let mut i = buf.len();
        while n >= 10 {
            i -= 1;
            buf[i] = b'0' + (n % 10) as u8;
            n /= 10;
        }
        i -= 1;
        buf[i] = b'0' + n as u8;
        Self::new(unsafe { std::str::from_utf8_unchecked(&buf[i..]) })
    }

    #[inline]
    pub fn new(s: &str) -> Self {
        if s.len() <= SMALL_STR_CAP {
            let mut data = [0u8; SMALL_STR_CAP];
            data[..s.len()].copy_from_slice(s.as_bytes());
            Self {
                data,
                len: s.len() as u8,
            }
        } else {
            let ptr = Box::into_raw(s.to_owned().into_boxed_str());
            let mut data = [0u8; SMALL_STR_CAP];
            let bytes = (ptr as *const u8 as usize).to_ne_bytes();
            data[..8].copy_from_slice(&bytes);
            Self::store_heap_len(&mut data, s.len());
            Self { data, len: 0xFF }
        }
    }

    #[inline]
    pub fn from_string(s: String) -> Self {
        if s.len() <= SMALL_STR_CAP {
            let mut data = [0u8; SMALL_STR_CAP];
            data[..s.len()].copy_from_slice(s.as_bytes());
            let len = s.len() as u8;
            drop(s);
            Self { data, len }
        } else {
            let ptr = Box::into_raw(s.into_boxed_str());
            let mut data = [0u8; SMALL_STR_CAP];
            let bytes = (ptr as *const u8 as usize).to_ne_bytes();
            data[..8].copy_from_slice(&bytes);
            Self::store_heap_len(&mut data, unsafe { &*ptr }.len());
            Self { data, len: 0xFF }
        }
    }

    #[inline(always)]
    pub fn as_str(&self) -> &str {
        if self.len != 0xFF {
            unsafe { std::str::from_utf8_unchecked(&self.data[..self.len as usize]) }
        } else {
            let ptr_val = usize::from_ne_bytes(self.data[..8].try_into().unwrap());
            let len_val = Self::read_heap_len(&self.data);
            unsafe {
                let slice = std::slice::from_raw_parts(ptr_val as *const u8, len_val);
                std::str::from_utf8_unchecked(slice)
            }
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        if self.len != 0xFF {
            self.len as usize
        } else {
            Self::read_heap_len(&self.data)
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn into_string(self) -> String {
        if self.len != 0xFF {
            let s = self.as_str().to_owned();
            std::mem::forget(self);
            s
        } else {
            let ptr_val = usize::from_ne_bytes(self.data[..8].try_into().unwrap());
            let len_val = Self::read_heap_len(&self.data);
            let boxed = unsafe {
                Box::from_raw(
                    std::ptr::slice_from_raw_parts_mut(ptr_val as *mut u8, len_val) as *mut str,
                )
            };
            std::mem::forget(self);
            boxed.into()
        }
    }

    #[inline]
    pub fn push_str(&mut self, s: &str) {
        let cur = self.len();
        if self.len != 0xFF && cur + s.len() <= SMALL_STR_CAP {
            self.data[cur..cur + s.len()].copy_from_slice(s.as_bytes());
            self.len = (cur + s.len()) as u8;
            return;
        }
        let mut owned = self.as_str().to_owned();
        owned.push_str(s);
        *self = Self::from_string(owned);
    }

    #[inline]
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        if self.len != 0xFF {
            &mut self.data[..self.len as usize]
        } else {
            let ptr_val = usize::from_ne_bytes(self.data[..8].try_into().unwrap());
            let len_val = Self::read_heap_len(&self.data);
            unsafe { std::slice::from_raw_parts_mut(ptr_val as *mut u8, len_val) }
        }
    }

    #[inline]
    pub fn into_bytes(self) -> Vec<u8> {
        self.into_string().into_bytes()
    }

    #[inline]
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self::from_string(unsafe { String::from_utf8_unchecked(bytes) })
    }

    #[inline]
    pub fn ensure_len(&mut self, min_len: usize) {
        if self.len() < min_len {
            let mut owned = self.as_str().to_owned();
            owned.extend(std::iter::repeat_n('\0', min_len - owned.len()));
            *self = Self::from_string(owned);
        }
    }
}

impl Drop for SmallStr {
    fn drop(&mut self) {
        if self.len == 0xFF {
            let ptr_val = usize::from_ne_bytes(self.data[..8].try_into().unwrap());
            let len_val = Self::read_heap_len(&self.data);
            unsafe {
                drop(Box::from_raw(
                    std::ptr::slice_from_raw_parts_mut(ptr_val as *mut u8, len_val) as *mut str,
                ));
            }
        }
    }
}

// Derived Clone would bitwise-copy the heap pointer, aliasing one allocation
// between two owners. Clone must deep-copy instead.
impl Clone for SmallStr {
    #[inline]
    fn clone(&self) -> Self {
        if self.len != 0xFF {
            Self {
                data: self.data,
                len: self.len,
            }
        } else {
            Self::new(self.as_str())
        }
    }
}

impl std::ops::Deref for SmallStr {
    type Target = str;
    #[inline(always)]
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl Default for SmallStr {
    fn default() -> Self {
        Self {
            data: [0u8; SMALL_STR_CAP],
            len: 0,
        }
    }
}

impl From<String> for SmallStr {
    fn from(s: String) -> Self {
        Self::from_string(s)
    }
}

impl From<&str> for SmallStr {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl std::fmt::Display for SmallStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::fmt::Debug for SmallStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "\"{}\"", self.as_str())
    }
}

impl PartialEq for SmallStr {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for SmallStr {}

impl std::hash::Hash for SmallStr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl std::borrow::Borrow<str> for SmallStr {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq<str> for SmallStr {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for SmallStr {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

#[cfg(test)]
mod tests {
    use super::SmallStr;

    #[test]
    fn clone_is_an_independent_copy() {
        let long = "0123456789abcdefghij";
        let a = SmallStr::new(long);
        let b = a.clone();
        assert_eq!(a.as_str(), long);
        assert_eq!(b.as_str(), long);
        // Heap variant must not alias: two owners of one box would double-free.
        if long.len() > super::SMALL_STR_CAP {
            assert_ne!(
                a.as_str().as_ptr(),
                b.as_str().as_ptr(),
                "cloned heap variant aliases the original allocation"
            );
        }
        let mut c = a.clone();
        c.push_str("x");
        assert_eq!(a.as_str(), long);
        assert_eq!(c.as_str(), "0123456789abcdefghijx");
    }

    #[test]
    fn push_str_stays_inline_when_it_fits() {
        let mut s = SmallStr::new("hello");
        s.push_str(" world"); // 11 bytes total, fits inline
        assert_eq!(s.as_str(), "hello world");
    }
}
