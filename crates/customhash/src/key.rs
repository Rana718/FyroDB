pub(crate) const INLINE_CAP: usize = 15;

#[repr(C)]
pub(crate) struct CompactKey {
    data: [u8; INLINE_CAP],
    tag: u8,
}

impl CompactKey {
    #[inline]
    fn store_heap_len(data: &mut [u8; INLINE_CAP], len: usize) {
        let lb = (len as u64).to_ne_bytes();
        data[8..15].copy_from_slice(&lb[..7]);
    }

    #[inline(always)]
    fn read_heap_len(data: &[u8; INLINE_CAP]) -> usize {
        let mut lb = [0u8; 8];
        lb[..7].copy_from_slice(&data[8..15]);
        u64::from_ne_bytes(lb) as usize
    }

    pub(crate) fn from_string(s: String) -> Self {
        if s.len() <= INLINE_CAP {
            let mut data = [0u8; INLINE_CAP];
            data[..s.len()].copy_from_slice(s.as_bytes());
            Self {
                data,
                tag: s.len() as u8,
            }
        } else {
            let ptr = Box::into_raw(s.into_boxed_str());
            let mut data = [0u8; INLINE_CAP];
            let addr = (ptr as *const u8 as usize).to_ne_bytes();
            data[..8].copy_from_slice(&addr);
            Self::store_heap_len(&mut data, unsafe { &*ptr }.len());
            Self { data, tag: 0xFF }
        }
    }

/// No allocation for inline (≤15B) keys; one box for longer ones.
    #[inline]
    pub(crate) fn from_str(s: &str) -> Self {
        if s.len() <= INLINE_CAP {
            let mut data = [0u8; INLINE_CAP];
            data[..s.len()].copy_from_slice(s.as_bytes());
            Self {
                data,
                tag: s.len() as u8,
            }
        } else {
            Self::from_string(String::from(s))
        }
    }

    #[inline(always)]
    pub(crate) fn as_str(&self) -> &str {
        if self.tag != 0xFF {
            unsafe { std::str::from_utf8_unchecked(&self.data[..self.tag as usize]) }
        } else {
            let ptr_val = usize::from_ne_bytes(self.data[..8].try_into().unwrap());
            let len_val = Self::read_heap_len(&self.data);
            unsafe {
                std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                    ptr_val as *const u8,
                    len_val,
                ))
            }
        }
    }
}

impl Drop for CompactKey {
    fn drop(&mut self) {
        if self.tag == 0xFF {
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

impl PartialEq<&str> for CompactKey {
    #[inline(always)]
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl std::fmt::Display for CompactKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
