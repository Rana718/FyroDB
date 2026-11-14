pub(crate) const INLINE_CAP: usize = 15;

const HEAP: u8 = u8::MAX;

#[repr(C)]
pub struct CompactKey {
   data: [u8; INLINE_CAP],
   tag: u8,
}

impl CompactKey {
   #[inline(always)]
   fn fits_full_width(s: &str) -> bool {
      Self::fits_full_width_bytes(s.as_bytes())
   }

   #[inline(always)]
   fn fits_full_width_bytes(key: &[u8]) -> bool {
      key.len() == 16 && key[15] > INLINE_CAP as u8 && key[15] != HEAP
   }

   #[inline(always)]
   fn full_width(s: &str) -> Self {
      Self::full_width_bytes(s.as_bytes())
   }

   #[inline(always)]
   fn full_width_bytes(key: &[u8]) -> Self {
      let mut data = [0u8; INLINE_CAP];
      data.copy_from_slice(&key[..INLINE_CAP]);
      Self {
         data,
         tag: key[15],
      }
   }

   #[inline]
   fn store_heap_len(data: &mut [u8; INLINE_CAP], len: usize) {
      let lb = (len as u64).to_le_bytes();
      unsafe {
         std::ptr::copy_nonoverlapping(lb.as_ptr(), data.as_mut_ptr().add(8), 7);
      }
   }

   #[inline(always)]
   fn read_heap_len(data: &[u8; INLINE_CAP]) -> usize {
      unsafe {
         let word = std::ptr::read_unaligned(data.as_ptr().add(8).cast::<u64>());
         (word.to_le() & 0x00FF_FFFF_FFFF_FFFF) as usize
      }
   }

   pub(crate) fn from_string(s: String) -> Self {
      if s.len() <= INLINE_CAP {
         let mut data = [0u8; INLINE_CAP];
         unsafe {
            std::ptr::copy_nonoverlapping(s.as_bytes().as_ptr(), data.as_mut_ptr(), s.len());
         }
         Self {
            data,
            tag: s.len() as u8,
         }
      } else if Self::fits_full_width(&s) {
         Self::full_width(&s)
      } else {
         let ptr = Box::into_raw(s.into_boxed_str());
         let mut data = [0u8; INLINE_CAP];
         unsafe {
            std::ptr::write_unaligned(data.as_mut_ptr().cast::<usize>(), ptr as *const u8 as usize);
         }
         Self::store_heap_len(&mut data, unsafe { &*ptr }.len());
         Self { data, tag: HEAP }
      }
   }

   /// Full-width keys use their final byte when it is not a length or heap tag.
   #[inline(always)]
   #[allow(clippy::should_implement_trait)]
   pub fn from_str(s: &str) -> Self {
      Self::from_bytes(s.as_bytes())
   }

   /// Audit S-2: bytes-native key construction — identical layout rules as
   /// `from_str` (inline ≤15, eligible 16-byte full-width, heap otherwise),
   /// enabling binary keys end to end.
   #[inline(always)]
   pub fn from_bytes(key: &[u8]) -> Self {
      let len = key.len();
      if len <= INLINE_CAP {
         let mut data = [0u8; INLINE_CAP];
         unsafe {
            let src = key.as_ptr();
            let dst = data.as_mut_ptr();
            if len >= 8 {
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
            tag: len as u8,
         }
      } else if Self::fits_full_width_bytes(key) {
         Self::full_width_bytes(key)
      } else {
         Self::from_string(unsafe { String::from_utf8_unchecked(key.to_vec()) })
      }
   }

   #[inline(always)]
   pub fn as_str(&self) -> &str {
      if self.tag != HEAP {
         let length = if self.tag as usize <= INLINE_CAP {
            self.tag as usize
         } else {
            16
         };
         let bytes =
            unsafe { std::slice::from_raw_parts((self as *const Self).cast::<u8>(), length) };
         unsafe { std::str::from_utf8_unchecked(bytes) }
      } else {
         let ptr_val = unsafe { std::ptr::read_unaligned(self.data.as_ptr().cast::<*const u8>()) };
         let len_val = Self::read_heap_len(&self.data);
         unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr_val, len_val)) }
      }
   }

   #[inline(always)]
   pub fn eq_bytes(&self, other: &[u8]) -> bool {
      let other_len = other.len();
      if other_len <= INLINE_CAP {
         if (self.tag as usize) != other_len {
            return false;
         }
         let a = self.data.as_ptr();
         let b = other.as_ptr();
         unsafe {
            if other_len >= 8 {
               let d1 = std::ptr::read_unaligned(a.cast::<u64>())
                  ^ std::ptr::read_unaligned(b.cast::<u64>());
               let d2 = std::ptr::read_unaligned(a.add(other_len - 8).cast::<u64>())
                  ^ std::ptr::read_unaligned(b.add(other_len - 8).cast::<u64>());
               (d1 | d2) == 0
            } else if other_len >= 4 {
               let d1 = std::ptr::read_unaligned(a.cast::<u32>())
                  ^ std::ptr::read_unaligned(b.cast::<u32>());
               let d2 = std::ptr::read_unaligned(a.add(other_len - 4).cast::<u32>())
                  ^ std::ptr::read_unaligned(b.add(other_len - 4).cast::<u32>());
               (d1 | d2) == 0
            } else if other_len == 0 {
               true
            } else if other_len == 1 {
               *a == *b
            } else {
               let d1 = (std::ptr::read_unaligned(a.cast::<u16>())
                  ^ std::ptr::read_unaligned(b.cast::<u16>())) as usize;
               let d2 = (*a.add(other_len - 1) ^ *b.add(other_len - 1)) as usize;
               (d1 | d2) == 0
            }
         }
      } else if self.tag == HEAP {
         let len_val = Self::read_heap_len(&self.data);
         if len_val != other_len {
            return false;
         }
         let a = unsafe { std::ptr::read_unaligned(self.data.as_ptr().cast::<*const u8>()) };
         let b = other.as_ptr();
         unsafe {
            if other_len <= 32 {
               let d1 = std::ptr::read_unaligned(a.cast::<u64>())
                  ^ std::ptr::read_unaligned(b.cast::<u64>());
               let d2 = std::ptr::read_unaligned(a.add(8).cast::<u64>())
                  ^ std::ptr::read_unaligned(b.add(8).cast::<u64>());
               let d3 = std::ptr::read_unaligned(a.add(other_len - 8).cast::<u64>())
                  ^ std::ptr::read_unaligned(b.add(other_len - 8).cast::<u64>());
               let d4 = if other_len > 24 {
                  std::ptr::read_unaligned(a.add(16).cast::<u64>())
                     ^ std::ptr::read_unaligned(b.add(16).cast::<u64>())
               } else {
                  0
               };
               (d1 | d2 | d3 | d4) == 0
            } else {
               std::slice::from_raw_parts(a, len_val) == other
            }
         }
      } else if other_len == 16 && self.tag as usize > INLINE_CAP {
         let a = (self as *const Self).cast::<u8>();
         let b = other.as_ptr();
         unsafe {
            let first = a.cast::<u64>().read_unaligned() ^ b.cast::<u64>().read_unaligned();
            let last =
               a.add(8).cast::<u64>().read_unaligned() ^ b.add(8).cast::<u64>().read_unaligned();
            (first | last) == 0
         }
      } else {
         false
      }
   }

   #[inline(always)]
   pub fn eq_str(&self, other: &str) -> bool {
      self.eq_bytes(other.as_bytes())
   }
}

impl Drop for CompactKey {
   fn drop(&mut self) {
      if self.tag == HEAP {
         let ptr_val = unsafe { std::ptr::read_unaligned(self.data.as_ptr().cast::<usize>()) };
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

#[cfg(test)]
mod tests {
   use super::*;

   #[test]
   fn all_length_tag_boundaries_and_ascii_endings_round_trip() {
      assert_eq!(std::mem::size_of::<CompactKey>(), 16);
      for length in [0, 1, 7, 8, 14, 15, 16, 17, 24, 31, 32, 33, 64] {
         for terminal in 0..128u8 {
            let mut bytes = vec![b'x'; length];
            if let Some(last) = bytes.last_mut() {
               *last = terminal;
            }
            let text = String::from_utf8(bytes).unwrap();
            for key in [
               CompactKey::from_str(&text),
               CompactKey::from_string(text.clone()),
            ] {
               assert_eq!(key.as_str(), text);
               assert!(key.eq_bytes(text.as_bytes()));
               assert!(!key.eq_bytes(&[text.as_bytes(), b"x"].concat()));
               if length > 0 {
                  assert!(!key.eq_bytes(&text.as_bytes()[..length - 1]));
               }
               let inline = length <= INLINE_CAP || (length == 16 && terminal > INLINE_CAP as u8);
               assert_eq!(
                  key.as_str().as_ptr() == (&key as *const CompactKey).cast(),
                  inline
               );
            }
         }
      }
   }

   #[test]
   fn full_width_unicode_keys_preserve_the_original_utf8_bytes() {
      for character in (0..=0x7ff).filter_map(char::from_u32).chain(['€', '😊']) {
         let text = format!("{}{character}", "k".repeat(16 - character.len_utf8()));
         for key in [
            CompactKey::from_str(&text),
            CompactKey::from_string(text.clone()),
         ] {
            assert_eq!(key.as_str().as_bytes(), text.as_bytes());
            assert!(key.eq_str(&text));
            let mut other = text.as_bytes().to_vec();
            other[15] ^= 0x80;
            assert!(!key.eq_bytes(&other));
            assert!(!key.eq_bytes(&text.as_bytes()[..15]));
         }
      }
   }

   #[test]
   fn full_width_keys_survive_growth_updates_scans_and_deletions() {
      let map = std::sync::Arc::new(crate::CustomMap::build(2, 8, usize::MAX));
      std::thread::scope(|scope| {
         for worker in 0..4u64 {
            let map = std::sync::Arc::clone(&map);
            scope.spawn(move || {
               for offset in 0..1000u64 {
                  let i = worker * 1000 + offset;
                  let key = format!("k:{i:014}");
                  if offset % 2 == 0 {
                     assert!(map.insert(key.clone(), i));
                  } else {
                     assert!(map.insert_str(&key, i));
                  }
                  assert_eq!(map.get(&key), Some(i));
               }
            });
         }
      });
      for i in 0..4000u64 {
         let key = format!("k:{i:014}");
         assert_eq!(map.get(&key), Some(i));
         assert!(!map.insert_str(&key, i + 1));
      }
      let mut snapshot = Vec::new();
      map.for_each(|key, value| snapshot.push((key.to_owned(), *value)));
      assert_eq!(snapshot.len(), 4000);
      for i in 0..4000u64 {
         assert_eq!(map.remove(&format!("k:{i:014}")), Some(i + 1));
      }
      for (key, value) in snapshot {
         assert_eq!(key.len(), 16);
         assert_eq!(key[2..].parse::<u64>().unwrap() + 1, value);
      }
   }
}
