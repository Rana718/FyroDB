pub(crate) const INLINE_CAP: usize = 15;

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
   Heap = 0xFF,
}

impl Tag {
   #[inline(always)]
   pub(crate) fn inline(n: usize) -> Tag {
      assert!(n <= 15);
      unsafe { std::mem::transmute::<u8, Tag>(n as u8) }
   }
}

#[repr(C)]
pub struct CompactKey {
   data: [u8; INLINE_CAP],
   tag: Tag,
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
            tag: Tag::inline(s.len()),
         }
      } else {
         let ptr = Box::into_raw(s.into_boxed_str());
         let mut data = [0u8; INLINE_CAP];
         let addr = (ptr as *const u8 as usize).to_ne_bytes();
         data[..8].copy_from_slice(&addr);
         Self::store_heap_len(&mut data, unsafe { &*ptr }.len());
         Self {
            data,
            tag: Tag::Heap,
         }
      }
   }

   /// No allocation for inline (≤15B) keys; one box for longer ones.
   #[inline(always)]
   #[allow(clippy::should_implement_trait)]
   pub fn from_str(s: &str) -> Self {
      let len = s.len();
      if len <= INLINE_CAP {
         let mut data = [0u8; INLINE_CAP];
         unsafe {
            std::ptr::copy_nonoverlapping(s.as_bytes().as_ptr(), data.as_mut_ptr(), len);
         }
         Self {
            data,
            tag: Tag::inline(len),
         }
      } else {
         Self::from_string(String::from(s))
      }
   }

   #[inline(always)]
   pub fn as_str(&self) -> &str {
      if self.tag != Tag::Heap {
         unsafe { std::str::from_utf8_unchecked(&self.data[..self.tag as usize]) }
      } else {
         let ptr_val = usize::from_ne_bytes(self.data[..8].try_into().unwrap());
         let len_val = Self::read_heap_len(&self.data);
         unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr_val as *const u8, len_val))
         }
      }
   }

   #[inline(always)]
   pub fn eq_str(&self, other: &str) -> bool {
      let other_len = other.len();
      if self.tag != Tag::Heap {
         let len = self.tag as usize;
         if len != other_len {
            return false;
         }
         let a = self.data.as_ptr();
         let b = other.as_bytes().as_ptr();
         unsafe {
            if len >= 8 {
               let w1 = std::ptr::read_unaligned(a.cast::<u64>()) == std::ptr::read_unaligned(b.cast::<u64>());
               let w2 = std::ptr::read_unaligned(a.add(len - 8).cast::<u64>())
                  == std::ptr::read_unaligned(b.add(len - 8).cast::<u64>());
               w1 && w2
            } else if len >= 4 {
               let w1 = std::ptr::read_unaligned(a.cast::<u32>()) == std::ptr::read_unaligned(b.cast::<u32>());
               let w2 = std::ptr::read_unaligned(a.add(len - 4).cast::<u32>())
                  == std::ptr::read_unaligned(b.add(len - 4).cast::<u32>());
               w1 && w2
            } else if len == 0 {
               true
            } else if len == 1 {
               *a == *b
            } else {
               let w1 = std::ptr::read_unaligned(a.cast::<u16>()) == std::ptr::read_unaligned(b.cast::<u16>());
               w1 && (*a.add(len - 1) == *b.add(len - 1))
            }
         }
      } else {
         let len_val = Self::read_heap_len(&self.data);
         if len_val != other_len {
            return false;
         }
         let ptr_val = usize::from_ne_bytes(self.data[..8].try_into().unwrap()) as *const u8;
         unsafe { std::slice::from_raw_parts(ptr_val, len_val) == other.as_bytes() }
      }
   }
}

impl Drop for CompactKey {
   fn drop(&mut self) {
      if self.tag == Tag::Heap {
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
      self.eq_str(other)
   }
}

impl std::fmt::Display for CompactKey {
   fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      f.write_str(self.as_str())
   }
}
