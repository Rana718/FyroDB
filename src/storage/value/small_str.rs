pub(crate) const SMALL_STR_CAP: usize = 23;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
#[allow(dead_code)]
pub(crate) enum Tag {
   L0,
   L1,
   L2,
   L3,
   L4,
   L5,
   L6,
   L7,
   L8,
   L9,
   L10,
   L11,
   L12,
   L13,
   L14,
   L15,
   L16,
   L17,
   L18,
   L19,
   L20,
   L21,
   L22,
   L23,
   Heap = 0xFF,
}

impl Tag {
   #[inline(always)]
   pub(crate) fn inline(n: usize) -> Tag {
      debug_assert!(n <= 23);
      unsafe { std::mem::transmute::<u8, Tag>(n as u8) }
   }
}

#[repr(C)]
pub struct SmallStr {
   data: [u8; SMALL_STR_CAP],
   len: Tag,
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
            len: Tag::inline(s.len()),
         }
      } else {
         let ptr = Box::into_raw(s.to_owned().into_boxed_str());
         let mut data = [0u8; SMALL_STR_CAP];
         let bytes = (ptr as *const u8 as usize).to_ne_bytes();
         data[..8].copy_from_slice(&bytes);
         Self::store_heap_len(&mut data, s.len());
         Self {
            data,
            len: Tag::Heap,
         }
      }
   }

   #[inline]
   pub fn from_string(s: String) -> Self {
      if s.len() <= SMALL_STR_CAP {
         let mut data = [0u8; SMALL_STR_CAP];
         data[..s.len()].copy_from_slice(s.as_bytes());
         let len = Tag::inline(s.len());
         drop(s);
         Self { data, len }
      } else {
         let ptr = Box::into_raw(s.into_boxed_str());
         let mut data = [0u8; SMALL_STR_CAP];
         let bytes = (ptr as *const u8 as usize).to_ne_bytes();
         data[..8].copy_from_slice(&bytes);
         Self::store_heap_len(&mut data, unsafe { &*ptr }.len());
         Self {
            data,
            len: Tag::Heap,
         }
      }
   }

   #[inline(always)]
   pub fn as_str(&self) -> &str {
      if self.len != Tag::Heap {
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
      if self.len != Tag::Heap {
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
      if self.len != Tag::Heap {
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

   #[inline(always)]
   pub fn overwrite_str(&mut self, s: &str) {
      let len = s.len();
      if (self.len as usize) <= SMALL_STR_CAP && len <= SMALL_STR_CAP {
         unsafe {
            if len <= 8 {
               if len >= 4 {
                  let w1 = std::ptr::read_unaligned(s.as_ptr().cast::<u32>());
                  let w2 = std::ptr::read_unaligned(s.as_ptr().add(len - 4).cast::<u32>());
                  std::ptr::write_unaligned(self.data.as_mut_ptr().cast::<u32>(), w1);
                  std::ptr::write_unaligned(self.data.as_mut_ptr().add(len - 4).cast::<u32>(), w2);
               } else if len > 0 {
                  for i in 0..len {
                     *self.data.as_mut_ptr().add(i) = *s.as_ptr().add(i);
                  }
               }
            } else {
               std::ptr::copy_nonoverlapping(s.as_ptr(), self.data.as_mut_ptr(), len);
            }
         }
         self.len = Tag::inline(len);
         return;
      }
      *self = Self::new(s);
   }

   #[inline]
   pub fn push_str(&mut self, s: &str) {
      let cur = self.len();
      if self.len != Tag::Heap && cur + s.len() <= SMALL_STR_CAP {
         self.data[cur..cur + s.len()].copy_from_slice(s.as_bytes());
         self.len = Tag::inline(cur + s.len());
         return;
      }
      let mut owned = self.as_str().to_owned();
      owned.push_str(s);
      *self = Self::from_string(owned);
   }

   #[inline]
   pub fn as_bytes_mut(&mut self) -> &mut [u8] {
      if self.len != Tag::Heap {
         &mut self.data[..self.len as usize]
      } else {
         let ptr_val = usize::from_ne_bytes(self.data[..8].try_into().unwrap());
         let len_val = Self::read_heap_len(&self.data);
         unsafe { std::slice::from_raw_parts_mut(ptr_val as *mut u8, len_val) }
      }
   }

   #[inline(always)]
   pub fn as_bytes(&self) -> &[u8] {
      if self.len != Tag::Heap {
         &self.data[..self.len as usize]
      } else {
         let ptr = usize::from_ne_bytes(self.data[..8].try_into().unwrap()) as *const u8;
         unsafe { std::slice::from_raw_parts(ptr, Self::read_heap_len(&self.data)) }
      }
   }

   #[inline(always)]
   pub fn write_resp(&self, out: &mut Vec<u8>) {
      let len = self.len as usize;
      if len < 10 {
         let old_len = out.len();
         let total = 6 + len;
         if out.capacity() < old_len + total {
            crate::utils::resp::write_bulk(out, self.as_str());
            return;
         }
         unsafe {
            let p = out.as_mut_ptr().add(old_len);
            p.cast::<u32>().write_unaligned(u32::from_ne_bytes([b'$', b'0' + len as u8, b'\r', b'\n']));
            p.add(4).cast::<u64>().write_unaligned(self.data.as_ptr().cast::<u64>().read_unaligned());
            p.add(4 + len).cast::<u16>().write_unaligned(u16::from_ne_bytes(*b"\r\n"));
            out.set_len(old_len + total);
         }
         return;
      }
      if len <= SMALL_STR_CAP {
         let old_len = out.len();
         let total = 7 + len;
         if out.capacity() < old_len + total {
            crate::utils::resp::write_bulk(out, self.as_str());
            return;
         }
         unsafe {
            let p = out.as_mut_ptr().add(old_len);
            let d1 = b'0' + (len / 10) as u8;
            let d2 = b'0' + (len % 10) as u8;
            std::ptr::copy_nonoverlapping([b'$', d1, d2, b'\r', b'\n'].as_ptr(), p, 5);
            std::ptr::copy_nonoverlapping(self.data.as_ptr(), p.add(5), len);
            p.add(5 + len).cast::<u16>().write_unaligned(u16::from_ne_bytes(*b"\r\n"));
            out.set_len(old_len + total);
         }
         return;
      }
      crate::utils::resp::write_bulk_bytes(out, self.as_bytes());
   }

   #[inline]
   pub fn into_bytes(self) -> Vec<u8> {
      if self.len != Tag::Heap {
         return self.as_bytes().to_vec();
      }
      let ptr = usize::from_ne_bytes(self.data[..8].try_into().unwrap()) as *mut u8;
      let len = Self::read_heap_len(&self.data);
      let bytes = unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len)) };
      std::mem::forget(self);
      bytes.into_vec()
   }

   #[inline]
   pub fn from_slice(bytes: &[u8]) -> Self {
      if bytes.len() <= SMALL_STR_CAP {
         let mut data = [0u8; SMALL_STR_CAP];
         data[..bytes.len()].copy_from_slice(bytes);
         Self {
            data,
            len: Tag::inline(bytes.len()),
         }
      } else {
         let len = bytes.len();
         let ptr = Box::into_raw(bytes.to_vec().into_boxed_slice()) as *mut u8;
         let mut data = [0u8; SMALL_STR_CAP];
         data[..8].copy_from_slice(&(ptr as usize).to_ne_bytes());
         Self::store_heap_len(&mut data, len);
         Self {
            data,
            len: Tag::Heap,
         }
      }
   }

   #[inline]
   pub fn from_bytes(bytes: Vec<u8>) -> Self {
      if bytes.len() <= SMALL_STR_CAP {
         let mut data = [0u8; SMALL_STR_CAP];
         data[..bytes.len()].copy_from_slice(&bytes);
         return Self {
            data,
            len: Tag::inline(bytes.len()),
         };
      }
      let len = bytes.len();
      let ptr = Box::into_raw(bytes.into_boxed_slice()) as *mut u8;
      let mut data = [0u8; SMALL_STR_CAP];
      data[..8].copy_from_slice(&(ptr as usize).to_ne_bytes());
      Self::store_heap_len(&mut data, len);
      Self {
         data,
         len: Tag::Heap,
      }
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
      if self.len == Tag::Heap {
         let ptr_val = usize::from_ne_bytes(self.data[..8].try_into().unwrap());
         let len_val = Self::read_heap_len(&self.data);
         unsafe {
            drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
               ptr_val as *mut u8,
               len_val,
            )));
         }
      }
   }
}

// Derived Clone would bitwise-copy the heap pointer, aliasing one allocation
// between two owners. Clone must deep-copy instead.
impl Clone for SmallStr {
   #[inline]
   fn clone(&self) -> Self {
      if self.len != Tag::Heap {
         Self {
            data: self.data,
            len: self.len,
         }
      } else {
         Self::from_slice(self.as_bytes())
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
         len: Tag::L0,
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
#[path = "../../../tests/unit/storage/small_str.rs"]
mod tests;
