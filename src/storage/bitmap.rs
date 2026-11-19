use crate::storage::store::Store;
use crate::storage::value::{FyroDB, SmallStr, StoreValue};

#[inline]
pub fn count_ones_fast(slice: &[u8]) -> usize {
   let (prefix, words, suffix) = unsafe { slice.align_to::<u64>() };
   let mut count: usize = 0;
   for &b in prefix {
      count += b.count_ones() as usize;
   }
   // Unroll 4x independent 64-bit word popcount accumulators for instruction-level parallelism
   let mut c0: usize = 0;
   let mut c1: usize = 0;
   let mut c2: usize = 0;
   let mut c3: usize = 0;
   let (chunks, remainder) = words.as_chunks::<4>();
   for chunk in chunks {
      c0 += chunk[0].count_ones() as usize;
      c1 += chunk[1].count_ones() as usize;
      c2 += chunk[2].count_ones() as usize;
      c3 += chunk[3].count_ones() as usize;
   }
   count += (c0 + c1) + (c2 + c3);
   for &w in remainder {
      count += w.count_ones() as usize;
   }
   for &b in suffix {
      count += b.count_ones() as usize;
   }
   count
}

impl Store {
   pub fn setbit(&self, key: &str, offset: u64, value: bool) -> Result<u8, &'static str> {
      self.setbit_bytes(key.as_bytes(), offset, value)
   }

   pub fn setbit_bytes(&self, key: &[u8], offset: u64, value: bool) -> Result<u8, &'static str> {
      let byte_idx = (offset / 8) as usize;
      let bit_idx = 7 - (offset % 8) as u8;

      let mut apply_bit = |val: &mut StoreValue| {
         if val.is_expired() {
            let mut s = SmallStr::zeroed(byte_idx + 1);
            if value {
               s.as_bytes_mut()[byte_idx] |= 1 << bit_idx;
            }
            val.value = FyroDB::String(s);
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
      };

      if let Some(r) = self
         .data
         .update_with_hinted_bytes(key, None, &mut apply_bit)
      {
         return r;
      }

      loop {
         let mut s = SmallStr::zeroed(byte_idx + 1);
         if value {
            s.as_bytes_mut()[byte_idx] |= 1 << bit_idx;
         }
         if self
            .data
            .insert_if_absent_bytes_hinted(key, None, StoreValue::string_small(s))
         {
            return Ok(0);
         }
         if let Some(r) = self
            .data
            .update_with_hinted_bytes(key, None, &mut apply_bit)
         {
            return r;
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
                  let s_byte = s_idx / 8;
                  let e_byte = e_idx / 8;
                  let count = if s_byte == e_byte {
                     let mask = (0xFF_u8 >> (s_idx % 8)) & (0xFF_u8 << (7 - e_idx % 8));
                     (bytes[s_byte] & mask).count_ones() as usize
                  } else {
                     let head = (bytes[s_byte] & (0xFF_u8 >> (s_idx % 8))).count_ones() as usize;
                     let mid: usize = count_ones_fast(&bytes[s_byte + 1..e_byte]);
                     let tail =
                        (bytes[e_byte] & (0xFF_u8 << (7 - e_idx % 8))).count_ones() as usize;
                     head + mid + tail
                  };
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
                  let count = count_ones_fast(&bytes[s_idx..=e_idx]);
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
                  let s_byte = s_idx / 8;
                  let e_byte = e_idx / 8;
                  for byte_i in s_byte..=e_byte {
                     if byte_i >= bytes.len() {
                        break;
                     }
                     let lo = if byte_i == s_byte { s_idx % 8 } else { 0 };
                     let hi = if byte_i == e_byte { e_idx % 8 } else { 7 };
                     let mask = (0xFF_u8 >> lo) & (0xFF_u8 << (7 - hi));
                     let target = if bit == 1 {
                        bytes[byte_i] & mask
                     } else {
                        !bytes[byte_i] & mask
                     };
                     if target != 0 {
                        return Ok((byte_i * 8 + target.leading_zeros() as usize) as i64);
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
                     let target = if bit == 1 { b } else { !b };
                     if target != 0 {
                        return Ok((byte_idx * 8 + target.leading_zeros() as usize) as i64);
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
      let _guard = customhash::pin();
      let mut refs = Vec::with_capacity(keys.len());
      for &k in keys {
         let r = self.data.get_ref_pinned(k, &_guard);
         if let Some(ref e) = r {
            if e.is_expired() {
               refs.push(None);
            } else if e.value.as_string().is_none() {
               return Err("WRONGTYPE");
            } else {
               refs.push(r);
            }
         } else {
            refs.push(None);
         }
      }

      let mut slices: Vec<&[u8]> = Vec::with_capacity(keys.len());
      for r in &refs {
         if let Some(e) = r {
            let b = e.value.as_string().unwrap().as_bytes();
            max_len = max_len.max(b.len());
            slices.push(b);
         } else {
            slices.push(&[][..]);
         }
      }

      if slices.is_empty() {
         self.data.insert_str(dest, StoreValue::string_str(""));
         return Ok(0);
      }

      let mut result = vec![0u8; max_len];

      match op {
         BitOp::And => {
            result.fill(0xFF);
            for buf in &slices {
               let common = buf.len().min(max_len);
               let (r_words, r_tail) = result[..common].split_at_mut(common & !7);
               let (b_words, b_tail) = buf[..common].split_at(common & !7);
               for (r, b) in r_words
                  .as_chunks_mut::<8>()
                  .0
                  .iter_mut()
                  .zip(b_words.as_chunks::<8>().0.iter())
               {
                  let rw = u64::from_ne_bytes(*r);
                  let bw = u64::from_ne_bytes(*b);
                  r.copy_from_slice(&(rw & bw).to_ne_bytes());
               }
               for (r, b) in r_tail.iter_mut().zip(b_tail.iter()) {
                  *r &= *b;
               }
               if common < max_len {
                  result[common..].fill(0);
               }
            }
         }
         BitOp::Or => {
            for buf in &slices {
               let common = buf.len().min(max_len);
               let (r_words, r_tail) = result[..common].split_at_mut(common & !7);
               let (b_words, b_tail) = buf[..common].split_at(common & !7);
               for (r, b) in r_words
                  .as_chunks_mut::<8>()
                  .0
                  .iter_mut()
                  .zip(b_words.as_chunks::<8>().0.iter())
               {
                  let rw = u64::from_ne_bytes(*r);
                  let bw = u64::from_ne_bytes(*b);
                  r.copy_from_slice(&(rw | bw).to_ne_bytes());
               }
               for (r, b) in r_tail.iter_mut().zip(b_tail.iter()) {
                  *r |= *b;
               }
            }
         }
         BitOp::Xor => {
            for buf in &slices {
               let common = buf.len().min(max_len);
               let (r_words, r_tail) = result[..common].split_at_mut(common & !7);
               let (b_words, b_tail) = buf[..common].split_at(common & !7);
               for (r, b) in r_words
                  .as_chunks_mut::<8>()
                  .0
                  .iter_mut()
                  .zip(b_words.as_chunks::<8>().0.iter())
               {
                  let rw = u64::from_ne_bytes(*r);
                  let bw = u64::from_ne_bytes(*b);
                  r.copy_from_slice(&(rw ^ bw).to_ne_bytes());
               }
               for (r, b) in r_tail.iter_mut().zip(b_tail.iter()) {
                  *r ^= *b;
               }
            }
         }
         BitOp::Not => {
            if slices.len() != 1 {
               return Err("BITOP NOT requires one source key");
            }
            let buf = slices[0];
            let common = buf.len().min(max_len);
            let (r_words, r_tail) = result[..common].split_at_mut(common & !7);
            let (b_words, b_tail) = buf[..common].split_at(common & !7);
            for (r, b) in r_words
               .as_chunks_mut::<8>()
               .0
               .iter_mut()
               .zip(b_words.as_chunks::<8>().0.iter())
            {
               let bw = u64::from_ne_bytes(*b);
               r.copy_from_slice(&(!bw).to_ne_bytes());
            }
            for (r, b) in r_tail.iter_mut().zip(b_tail.iter()) {
               *r = !*b;
            }
            if common < max_len {
               result[common..].fill(0xFF);
            }
         }
      }

      let len = result.len();
      self
         .data
         .insert_str(dest, StoreValue::string_small(SmallStr::from_bytes(result)));
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
