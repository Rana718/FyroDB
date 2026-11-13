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

#[inline(never)]
fn copy_medium_payload(destination: &mut [u8], source: &[u8]) {
   let len = source.len();
   debug_assert!((65..=256).contains(&len));
   if len <= 128 {
      destination[..64].copy_from_slice(&source[..64]);
      destination[len - 64..len].copy_from_slice(&source[len - 64..len]);
   } else {
      destination[..128].copy_from_slice(&source[..128]);
      destination[len - 128..len].copy_from_slice(&source[len - 128..len]);
   }
}

#[inline(never)]
fn copy_large_payload(destination: &mut [u8], source: &[u8]) {
   assert_eq!(destination.len(), source.len());
   #[cfg(target_arch = "x86_64")]
   if source.len() >= 8192 && std::is_x86_feature_detected!("avx") {
      // The helper fences streaming stores before the caller can reuse the destination.
      unsafe { copy_streaming_payload(destination, source) };
      return;
   }
   destination.copy_from_slice(source);
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
unsafe fn copy_streaming_payload(destination: &mut [u8], source: &[u8]) {
   use std::arch::x86_64::{_mm_sfence, _mm256_loadu_si256, _mm256_stream_si256};
   unsafe {
      let dst = destination.as_mut_ptr();
      let src = source.as_ptr();
      let len = source.len();
      let head = dst.align_offset(64);
      std::ptr::copy_nonoverlapping(src, dst, head);
      let mut at = head;
      while at + 128 <= len {
         for offset in [0, 32, 64, 96] {
            let value = _mm256_loadu_si256(src.add(at + offset).cast());
            _mm256_stream_si256(dst.add(at + offset).cast(), value);
         }
         at += 128;
      }
      if at + 64 <= len {
         for offset in [0, 32] {
            let value = _mm256_loadu_si256(src.add(at + offset).cast());
            _mm256_stream_si256(dst.add(at + offset).cast(), value);
         }
         at += 64;
      }
      _mm_sfence();
      std::ptr::copy_nonoverlapping(src.add(at), dst.add(at), len - at);
   }
}

const CRLF_U16: u16 = u16::from_ne_bytes(*b"\r\n");

impl SmallStr {
   #[inline]
   fn store_heap_len(data: &mut [u8; SMALL_STR_CAP], len: usize) {
      let lb = (len as u64).to_le_bytes();
      data[8..15].copy_from_slice(&lb[..7]);
   }

   #[inline(always)]
   fn read_heap_len(data: &[u8; SMALL_STR_CAP]) -> usize {
      unsafe {
         let word = std::ptr::read_unaligned(data.as_ptr().add(8).cast::<u64>());
         (word.to_le() & 0x00FF_FFFF_FFFF_FFFF) as usize
      }
   }

   /// Formats an integer into the inline buffer. i64 never exceeds 20 ASCII
   /// digits, so this allocates only when the digits exceed SMALL_STR_CAP.
   #[inline]
   pub fn from_int(n: i64) -> Self {
      let mut buf = [0u8; 20];
      Self::new(crate::utils::int_format::write_i64_to(&mut buf, n))
   }

   #[inline]
   pub fn from_float(f: f64) -> Self {
      let mut buffer = ryu::Buffer::new();
      Self::new(buffer.format(f))
   }

   #[inline]
   pub fn new(s: &str) -> Self {
      Self::from_slice(s.as_bytes())
   }

   #[inline]
   pub fn from_string(s: String) -> Self {
      if s.len() <= SMALL_STR_CAP {
         let mut data = [0u8; SMALL_STR_CAP];
         data[..s.len()].copy_from_slice(s.as_bytes());
         let len = Tag::inline(s.len());
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

   #[inline]
   pub fn try_as_str(&self) -> Result<&str, std::str::Utf8Error> {
      std::str::from_utf8(self.as_bytes())
   }

   /// Panics for non-UTF-8 values; binary callers must use as_bytes or try_as_str.
   #[inline]
   pub fn as_str(&self) -> &str {
      self.try_as_str().expect("binary SmallStr is not UTF-8")
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
      String::from_utf8(self.into_bytes()).expect("binary SmallStr is not UTF-8")
   }

   #[inline(always)]
   pub fn overwrite_bytes(&mut self, s: &[u8]) {
      let len = s.len();
      if (self.len as usize) <= SMALL_STR_CAP && len <= SMALL_STR_CAP {
         unsafe {
            let src = s.as_ptr();
            let dst = self.data.as_mut_ptr();
            if len >= 16 {
               let w1 = std::ptr::read_unaligned(src.cast::<u64>());
               let w2 = std::ptr::read_unaligned(src.add(8).cast::<u64>());
               let w3 = std::ptr::read_unaligned(src.add(len - 8).cast::<u64>());
               std::ptr::write_unaligned(dst.cast::<u64>(), w1);
               std::ptr::write_unaligned(dst.add(8).cast::<u64>(), w2);
               std::ptr::write_unaligned(dst.add(len - 8).cast::<u64>(), w3);
            } else if len >= 8 {
               let w1 = std::ptr::read_unaligned(src.cast::<u64>());
               let w2 = std::ptr::read_unaligned(src.add(len - 8).cast::<u64>());
               std::ptr::write_unaligned(dst.cast::<u64>(), w1);
               std::ptr::write_unaligned(dst.add(len - 8).cast::<u64>(), w2);
            } else if len >= 4 {
               let w1 = std::ptr::read_unaligned(src.cast::<u32>());
               let w2 = std::ptr::read_unaligned(src.add(len - 4).cast::<u32>());
               std::ptr::write_unaligned(dst.cast::<u32>(), w1);
               std::ptr::write_unaligned(dst.add(len - 4).cast::<u32>(), w2);
            } else if len == 1 {
               *dst = *src;
            } else if len == 2 {
               std::ptr::write_unaligned(
                  dst.cast::<u16>(),
                  std::ptr::read_unaligned(src.cast::<u16>()),
               );
            } else if len == 3 {
               std::ptr::write_unaligned(
                  dst.cast::<u16>(),
                  std::ptr::read_unaligned(src.cast::<u16>()),
               );
               *dst.add(2) = *src.add(2);
            }
         }
         self.len = Tag::inline(len);
         return;
      }
      if self.len == Tag::Heap && Self::read_heap_len(&self.data) == len {
         let destination = self.as_bytes_mut();
         if (32..=64).contains(&len) {
            destination[..32].copy_from_slice(&s[..32]);
            destination[len - 32..len].copy_from_slice(&s[len - 32..len]);
         } else if (65..=256).contains(&len) {
            copy_medium_payload(destination, s);
         } else {
            destination.copy_from_slice(s);
         }
         return;
      }
      *self = Self::from_slice(s);
   }

   #[inline(always)]
   pub(crate) fn overwrite_bytes_streaming(&mut self, bytes: &[u8]) {
      if bytes.len() >= 8192
         && self.len == Tag::Heap
         && Self::read_heap_len(&self.data) == bytes.len()
      {
         copy_large_payload(self.as_bytes_mut(), bytes);
      } else {
         self.overwrite_bytes(bytes);
      }
   }

   #[inline(always)]
   pub fn overwrite_str(&mut self, s: &str) {
      self.overwrite_bytes(s.as_bytes());
   }

   #[inline]
   pub fn push_str(&mut self, s: &str) {
      let cur = self.len();
      if self.len != Tag::Heap && cur + s.len() <= SMALL_STR_CAP {
         self.data[cur..cur + s.len()].copy_from_slice(s.as_bytes());
         self.len = Tag::inline(cur + s.len());
         return;
      }
      let mut owned = Vec::with_capacity(cur + s.len());
      owned.extend_from_slice(self.as_bytes());
      owned.extend_from_slice(s.as_bytes());
      *self = Self::from_bytes(owned);
   }

   #[inline]
   pub fn as_bytes_mut(&mut self) -> &mut [u8] {
      if self.len != Tag::Heap {
         &mut self.data[..self.len as usize]
      } else {
         let ptr_val = unsafe { std::ptr::read_unaligned(self.data.as_ptr().cast::<*mut u8>()) };
         let len_val = Self::read_heap_len(&self.data);
         unsafe { std::slice::from_raw_parts_mut(ptr_val, len_val) }
      }
   }

   #[inline(always)]
   pub fn as_bytes(&self) -> &[u8] {
      if self.len != Tag::Heap {
         &self.data[..self.len as usize]
      } else {
         let ptr = unsafe { std::ptr::read_unaligned(self.data.as_ptr().cast::<*const u8>()) };
         unsafe { std::slice::from_raw_parts(ptr, Self::read_heap_len(&self.data)) }
      }
   }

   #[inline(always)]
   pub(crate) fn prefetch_payload(&self) {
      let bytes = self.as_bytes();
      if !(24..=1024).contains(&bytes.len()) {
         return;
      }
      for pointer in [bytes.as_ptr(), bytes.as_ptr().wrapping_add(bytes.len() - 1)] {
         #[cfg(target_arch = "x86_64")]
         unsafe {
            std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(pointer.cast());
         }
         #[cfg(target_arch = "aarch64")]
         unsafe {
            std::arch::asm!("prfm pldl1keep, [{0}]", in(reg) pointer, options(nostack, readonly, preserves_flags));
         }
         #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
         let _ = pointer;
      }
   }

   #[inline(always)]
   pub fn write_resp(&self, out: &mut Vec<u8>) {
      let len = self.len as usize;
      if len < 10 {
         let old_len = out.len();
         if old_len + 16 <= out.capacity() {
            let total = 6 + len;
            unsafe {
               let p = out.as_mut_ptr().add(old_len);
               let header = 0x0A0D_3024u32 | ((len as u32) << 8);
               p.cast::<u32>().write_unaligned(header);
               p.add(4)
                  .cast::<u64>()
                  .write_unaligned(self.data.as_ptr().cast::<u64>().read_unaligned());
               if len == 9 {
                  *p.add(12) = *self.data.as_ptr().add(8);
               }
               p.add(4 + len).cast::<u16>().write_unaligned(CRLF_U16);
               out.set_len(old_len + total);
            }
            return;
         }
         crate::utils::resp::write_bulk_bytes(out, self.as_bytes());
         return;
      }
      if len <= SMALL_STR_CAP {
         let old_len = out.len();
         let total = 7 + len;
         if old_len + total <= out.capacity() {
            unsafe {
               let p = out.as_mut_ptr().add(old_len);
               let (d1, d2) = if len >= 20 {
                  (b'2', b'0' + (len - 20) as u8)
               } else {
                  (b'1', b'0' + (len - 10) as u8)
               };
               let header = 0x0D00_0024u32 | ((d1 as u32) << 8) | ((d2 as u32) << 16);
               p.cast::<u32>().write_unaligned(header);
               *p.add(4) = b'\n';

               let src = self.data.as_ptr();
               let dst = p.add(5);
               if len >= 16 {
                  let w1 = std::ptr::read_unaligned(src.cast::<u64>());
                  let w2 = std::ptr::read_unaligned(src.add(8).cast::<u64>());
                  let w3 = std::ptr::read_unaligned(src.add(len - 8).cast::<u64>());
                  std::ptr::write_unaligned(dst.cast::<u64>(), w1);
                  std::ptr::write_unaligned(dst.add(8).cast::<u64>(), w2);
                  std::ptr::write_unaligned(dst.add(len - 8).cast::<u64>(), w3);
               } else {
                  let w1 = std::ptr::read_unaligned(src.cast::<u64>());
                  let w2 = std::ptr::read_unaligned(src.add(len - 8).cast::<u64>());
                  std::ptr::write_unaligned(dst.cast::<u64>(), w1);
                  std::ptr::write_unaligned(dst.add(len - 8).cast::<u64>(), w2);
               }

               p.add(5 + len).cast::<u16>().write_unaligned(CRLF_U16);
               out.set_len(old_len + total);
            }
            return;
         }
         crate::utils::resp::write_bulk_bytes(out, self.as_bytes());
         return;
      }
      crate::utils::resp::write_bulk_bytes(out, self.as_bytes());
   }

   /// Writes the formatted RESP bulk string response directly to `p` without bounds checks.
   /// Returns the total bytes written.
   ///
   /// # Safety
   ///
   /// `p` must have at least `max(64, self.len() + 32)` writable bytes,
   /// including the entire heap-backed payload and its RESP framing.
   #[inline(always)]
   pub unsafe fn write_resp_raw(&self, p: *mut u8) -> usize {
      unsafe {
         let len = self.len as usize;
         if len < 10 {
            let total = 6 + len;
            let header = 0x0A0D_3024u32 | ((len as u32) << 8);
            p.cast::<u32>().write_unaligned(header);
            p.add(4)
               .cast::<u64>()
               .write_unaligned(self.data.as_ptr().cast::<u64>().read_unaligned());
            if len == 9 {
               *p.add(12) = *self.data.as_ptr().add(8);
            }
            p.add(4 + len).cast::<u16>().write_unaligned(CRLF_U16);
            total
         } else if len <= SMALL_STR_CAP {
            let total = 7 + len;
            let (d1, d2) = if len >= 20 {
               (b'2', b'0' + (len - 20) as u8)
            } else {
               (b'1', b'0' + (len - 10) as u8)
            };
            let header = 0x0D00_0024u32 | ((d1 as u32) << 8) | ((d2 as u32) << 16);
            p.cast::<u32>().write_unaligned(header);
            *p.add(4) = b'\n';

            let src = self.data.as_ptr();
            let dst = p.add(5);
            if len >= 16 {
               let w1 = std::ptr::read_unaligned(src.cast::<u64>());
               let w2 = std::ptr::read_unaligned(src.add(8).cast::<u64>());
               let w3 = std::ptr::read_unaligned(src.add(len - 8).cast::<u64>());
               std::ptr::write_unaligned(dst.cast::<u64>(), w1);
               std::ptr::write_unaligned(dst.add(8).cast::<u64>(), w2);
               std::ptr::write_unaligned(dst.add(len - 8).cast::<u64>(), w3);
            } else {
               let w1 = std::ptr::read_unaligned(src.cast::<u64>());
               let w2 = std::ptr::read_unaligned(src.add(len - 8).cast::<u64>());
               std::ptr::write_unaligned(dst.cast::<u64>(), w1);
               std::ptr::write_unaligned(dst.add(len - 8).cast::<u64>(), w2);
            }

            p.add(5 + len).cast::<u16>().write_unaligned(CRLF_U16);
            total
         } else {
            crate::utils::resp::write_bulk_raw(p, self.as_bytes())
         }
      }
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

   #[inline(always)]
   pub fn from_slice(bytes: &[u8]) -> Self {
      let len = bytes.len();
      if len <= SMALL_STR_CAP {
         let mut data = [0u8; SMALL_STR_CAP];
         unsafe {
            let src = bytes.as_ptr();
            let dst = data.as_mut_ptr();
            if len >= 16 {
               let w1 = std::ptr::read_unaligned(src.cast::<u64>());
               let w2 = std::ptr::read_unaligned(src.add(8).cast::<u64>());
               let w3 = std::ptr::read_unaligned(src.add(len - 8).cast::<u64>());
               std::ptr::write_unaligned(dst.cast::<u64>(), w1);
               std::ptr::write_unaligned(dst.add(8).cast::<u64>(), w2);
               std::ptr::write_unaligned(dst.add(len - 8).cast::<u64>(), w3);
            } else if len >= 8 {
               let w1 = std::ptr::read_unaligned(src.cast::<u64>());
               let w2 = std::ptr::read_unaligned(src.add(len - 8).cast::<u64>());
               std::ptr::write_unaligned(dst.cast::<u64>(), w1);
               std::ptr::write_unaligned(dst.add(len - 8).cast::<u64>(), w2);
            } else if len >= 4 {
               let w1 = std::ptr::read_unaligned(src.cast::<u32>());
               let w2 = std::ptr::read_unaligned(src.add(len - 4).cast::<u32>());
               std::ptr::write_unaligned(dst.cast::<u32>(), w1);
               std::ptr::write_unaligned(dst.add(len - 4).cast::<u32>(), w2);
            } else if len == 1 {
               *dst = *src;
            } else if len == 2 {
               std::ptr::write_unaligned(
                  dst.cast::<u16>(),
                  std::ptr::read_unaligned(src.cast::<u16>()),
               );
            } else if len == 3 {
               std::ptr::write_unaligned(
                  dst.cast::<u16>(),
                  std::ptr::read_unaligned(src.cast::<u16>()),
               );
               *dst.add(2) = *src.add(2);
            }
         }
         Self {
            data,
            len: Tag::inline(len),
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
         if self.len != Tag::Heap && min_len <= SMALL_STR_CAP {
            let current = self.len();
            self.data[current..min_len].fill(0);
            self.len = Tag::inline(min_len);
            return;
         }
         let mut owned = Vec::with_capacity(min_len);
         owned.extend_from_slice(self.as_bytes());
         owned.resize(min_len, 0);
         *self = Self::from_bytes(owned);
      }
   }
}

impl Drop for SmallStr {
   fn drop(&mut self) {
      if self.len == Tag::Heap {
         let ptr_val = unsafe { std::ptr::read_unaligned(self.data.as_ptr().cast::<*mut u8>()) };
         let len_val = Self::read_heap_len(&self.data);
         unsafe {
            drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
               ptr_val, len_val,
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
      f.write_str(&String::from_utf8_lossy(self.as_bytes()))
   }
}

impl std::fmt::Debug for SmallStr {
   fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      write!(f, "{:?}", String::from_utf8_lossy(self.as_bytes()))
   }
}

impl PartialEq for SmallStr {
   fn eq(&self, other: &Self) -> bool {
      self.as_bytes() == other.as_bytes()
   }
}

impl Eq for SmallStr {}

impl std::hash::Hash for SmallStr {
   fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
      match self.try_as_str() {
         Ok(text) => text.hash(state),
         Err(_) => self.as_bytes().hash(state),
      }
   }
}

impl std::borrow::Borrow<str> for SmallStr {
   fn borrow(&self) -> &str {
      self.as_str()
   }
}

impl AsRef<str> for SmallStr {
   #[inline(always)]
   fn as_ref(&self) -> &str {
      self.as_str()
   }
}

impl PartialEq<str> for SmallStr {
   fn eq(&self, other: &str) -> bool {
      self.as_bytes() == other.as_bytes()
   }
}

impl PartialEq<&str> for SmallStr {
   fn eq(&self, other: &&str) -> bool {
      self.as_bytes() == other.as_bytes()
   }
}

impl PartialEq<String> for SmallStr {
   #[inline]
   fn eq(&self, other: &String) -> bool {
      self.as_bytes() == other.as_bytes()
   }
}

impl PartialEq<SmallStr> for String {
   #[inline]
   fn eq(&self, other: &SmallStr) -> bool {
      self.as_bytes() == other.as_bytes()
   }
}

impl PartialEq<SmallStr> for &str {
   #[inline]
   fn eq(&self, other: &SmallStr) -> bool {
      self.as_bytes() == other.as_bytes()
   }
}

#[cfg(test)]
#[path = "../../../tests/unit/storage/small_str.rs"]
mod tests;
