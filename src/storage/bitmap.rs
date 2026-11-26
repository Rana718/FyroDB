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
               Ok(bytes.get(byte_idx).map_or(0, |b| (b >> bit_idx) & 1))
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
                  let count = bytes.get(s_idx..=e_idx).map_or(0, count_ones_fast);
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
      bit: bool,
      start: i64,
      end: i64,
      has_end: bool,
      use_bit: bool,
   ) -> Result<i64, &'static str> {
      match self.data.get_ref(key) {
         None => Ok(if bit { -1 } else { 0 }),
         Some(e) if e.is_expired() => Ok(if bit { -1 } else { 0 }),
         Some(e) => match e.value.as_string() {
            Some(s) => {
               let bytes = s.as_bytes();
               if bytes.is_empty() {
                  return Ok(if bit { -1 } else { 0 });
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
                  let mut lo_mask = 0xFF_u8 >> (s_idx % 8);
                  let mut hi_mask = 0xFF_u8;
                  for byte_i in s_byte..=e_byte.min(bytes.len() - 1) {
                     if byte_i == e_byte {
                        hi_mask = 0xFF_u8 >> (7 - (e_idx % 8));
                     }
                     let mask = lo_mask & hi_mask;
                     let target = if bit {
                        bytes[byte_i] & mask
                     } else {
                        !bytes[byte_i] & mask
                     };
                     if target != 0 {
                        return Ok((byte_i * 8 + target.leading_zeros() as usize) as i64);
                     }
                     lo_mask = 0xFF_u8;
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
                     let target = if bit { b } else { !b };
                     if target != 0 {
                        return Ok((byte_idx * 8 + target.leading_zeros() as usize) as i64);
                     }
                  }
                  if !bit && !has_end {
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
      let _guard = customhash::pin();
      if keys.is_empty() {
         self.data.insert_str(dest, StoreValue::string_str(""));
         return Ok(0);
      }

      let refs: Vec<_> = keys
         .iter()
         .map(|&key| self.data.get_ref_pinned(key, &_guard))
         .collect();
      let mut max_len = 0;
      let slices = refs
         .iter()
         .map(|entry| {
            let bytes = match entry {
               None => &[][..],
               Some(e) if e.is_expired() => &[][..],
               Some(e) => e.value.as_string().ok_or("WRONGTYPE")?.as_bytes(),
            };
            max_len = max_len.max(bytes.len());
            Ok(bytes)
         })
         .collect::<Result<Vec<_>, &'static str>>()?;

      let mut result = vec![0xFF * matches!(op, BitOp::And | BitOp::Not) as u8; max_len];
      match op {
         BitOp::And => {
            let mut min_processed = usize::MAX;
            for buf in &slices {
               let common = combine::<BIT_AND>(&mut result, buf);
               min_processed = min_processed.min(common);
            }
            result[min_processed..].fill(0);
         }
         BitOp::Or => {
            for buf in &slices {
               let _ = combine::<BIT_OR>(&mut result, buf);
            }
         }
         BitOp::Xor => {
            for buf in &slices {
               let _ = combine::<BIT_XOR>(&mut result, buf);
            }
         }
         BitOp::Not => {
            if keys.len() != 1 {
               return Err("BITOP NOT requires one source key");
            }
            let _ = combine::<BIT_NOT>(&mut result, slices[0]);
         }
      }

      drop(refs);
      let len = result.len();
      self
         .data
         .insert_str(dest, StoreValue::string_small(SmallStr::from_bytes(result)));
      Ok(len)
   }

   pub fn bf_get(
      &self,
      key: &str,
      offset: u64,
      bits: u32,
      signed: bool,
   ) -> Result<i64, &'static str> {
      if bits == 0 {
         return Ok(0);
      }
      if bits > 64 {
         return Err("invalid bitfield type");
      }
      match self.data.get_ref(key) {
         None => Ok(0),
         Some(e) if e.is_expired() => Ok(0),
         Some(e) => match e.value.as_string() {
            Some(s) => {
               let bytes = s.as_bytes();
               let start_bit = offset;
               let end_bit = offset + bits as u64;
               let start_byte = (start_bit / 8) as usize;
               let end_byte = end_bit.div_ceil(8) as usize;
               let mut val: u64 = 0;

               for b_idx in start_byte..end_byte {
                  let byte_val = if b_idx < bytes.len() { bytes[b_idx] } else { 0 };
                  let b_start = start_bit.max(b_idx as u64 * 8);
                  let b_end = end_bit.min(b_idx as u64 * 8 + 8);
                  let num_bits = (b_end - b_start) as u32;
                  let shift_right = (b_idx as u64 * 8 + 8 - b_end) as u32;
                  let mask = ((1u16 << num_bits) - 1) as u8;
                  let extracted_bits = ((byte_val >> shift_right) & mask) as u64;
                  let shift_left = (end_bit - b_end) as u32;
                  val |= extracted_bits << shift_left;
               }

               Ok(if signed && bits < 64 && (val >> (bits - 1)) & 1 == 1 {
                  val | (!0u64 << bits)
               } else {
                  val
               } as i64)
            }
            None => Err("WRONGTYPE"),
         },
      }
   }

   pub fn bf_set(&self, key: &str, offset: u64, bits: u32, value: i64) -> Result<(), &'static str> {
      if bits == 0 {
         return Ok(());
      }
      if bits > 64 {
         return Err("invalid bitfield type");
      }

      let last_byte_idx = ((offset + bits as u64 - 1) / 8) as usize;
      let val = value as u64;

      let write_bits = |bytes: &mut [u8]| {
         let start_bit = offset;
         let end_bit = offset + bits as u64;
         let start_byte = (start_bit / 8) as usize;
         let end_byte = end_bit.div_ceil(8) as usize;

         for (offset_idx, byte) in bytes[start_byte..end_byte].iter_mut().enumerate() {
            let b_idx = start_byte + offset_idx;
            let b_start = start_bit.max(b_idx as u64 * 8);
            let b_end = end_bit.min(b_idx as u64 * 8 + 8);
            let num_bits = (b_end - b_start) as u32;
            let val_shift_right = (end_bit - b_end) as u32;
            let bits_to_write =
               ((val >> val_shift_right) & (((1u16 << num_bits) - 1) as u64)) as u8;
            let byte_shift_left = (b_idx as u64 * 8 + 8 - b_end) as u32;
            let shifted_bits = bits_to_write << byte_shift_left;
            let mask = (((1u16 << num_bits) - 1) as u8) << byte_shift_left;
            *byte = (*byte & !mask) | shifted_bits;
         }
      };

      let result = self.data.update_with(key, |v| {
         let is_expired = v.is_expired();
         match v.value.as_string_mut() {
            Some(s) => {
               s.ensure_len(last_byte_idx + 1);
               let bytes = s.as_bytes_mut();
               if is_expired {
                  bytes.fill(0);
                  v.expires_ms = 0;
               }
               write_bits(bytes);
               Ok(())
            }
            None => Err("WRONGTYPE"),
         }
      });

      match result {
         Some(r) => r,
         None => {
            let mut s = SmallStr::zeroed(last_byte_idx + 1);
            write_bits(s.as_bytes_mut());
            self
               .data
               .insert(key.to_string(), StoreValue::string_small(s));
            Ok(())
         }
      }
   }
}

#[derive(Clone, Copy)]
pub enum BitOp {
   And,
   Or,
   Xor,
   Not,
}

const BIT_AND: u8 = 0;
const BIT_OR: u8 = 1;
const BIT_XOR: u8 = 2;
const BIT_NOT: u8 = 3;

#[inline(always)]
fn combine<const OP: u8>(result: &mut [u8], buf: &[u8]) -> usize {
   let common = result.len().min(buf.len());
   let result = &mut result[..common];
   let buf = &buf[..common];
   let mut processed = 0;

   #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
   {
      if common >= 64 && std::is_x86_feature_detected!("avx512f") {
         // SAFETY: AVX-512F was detected and both slices have the same length.
         processed = unsafe { combine_avx512f::<OP>(result, buf) };
      }
      if common - processed >= 32 && std::is_x86_feature_detected!("avx2") {
         // SAFETY: AVX2 was detected and both remaining slices have the same length.
         processed += unsafe { combine_avx2::<OP>(&mut result[processed..], &buf[processed..]) };
      }
   }
   #[cfg(target_arch = "aarch64")]
   if common >= 16 && std::arch::is_aarch64_feature_detected!("neon") {
      // SAFETY: NEON was detected and both slices have the same length.
      processed = unsafe { combine_neon::<OP>(result, buf) };
   }

   combine_scalar::<OP>(&mut result[processed..], &buf[processed..]);
   common
}

#[inline(always)]
fn combine_scalar<const OP: u8>(result: &mut [u8], buf: &[u8]) {
   let (r_words, r_tail) = result.as_chunks_mut::<8>();
   let (b_words, b_tail) = buf.as_chunks::<8>();
   for i in 0..r_words.len().min(b_words.len()) {
      let a = u64::from_ne_bytes(r_words[i]);
      let b = u64::from_ne_bytes(b_words[i]);
      r_words[i] = match OP {
         BIT_AND => a & b,
         BIT_OR => a | b,
         BIT_XOR => a ^ b,
         BIT_NOT => !b,
         _ => unreachable!(),
      }
      .to_ne_bytes();
   }
   for i in 0..r_tail.len().min(b_tail.len()) {
      r_tail[i] = match OP {
         BIT_AND => r_tail[i] & b_tail[i],
         BIT_OR => r_tail[i] | b_tail[i],
         BIT_XOR => r_tail[i] ^ b_tail[i],
         BIT_NOT => !b_tail[i],
         _ => unreachable!(),
      };
   }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx512f")]
unsafe fn combine_avx512f<const OP: u8>(result: &mut [u8], buf: &[u8]) -> usize {
   #[cfg(target_arch = "x86")]
   use std::arch::x86::*;
   #[cfg(target_arch = "x86_64")]
   use std::arch::x86_64::*;

   let end = result.len().min(buf.len()) & !63;
   for i in (0..end).step_by(64) {
      // SAFETY: Each unaligned load/store accesses 64 bytes within both slices.
      unsafe {
         let b = _mm512_loadu_si512(buf.as_ptr().add(i).cast());
         let value = if OP == BIT_NOT {
            _mm512_xor_si512(b, _mm512_set1_epi32(-1))
         } else {
            let a = _mm512_loadu_si512(result.as_ptr().add(i).cast());
            match OP {
               BIT_AND => _mm512_and_si512(a, b),
               BIT_OR => _mm512_or_si512(a, b),
               BIT_XOR => _mm512_xor_si512(a, b),
               _ => unreachable!(),
            }
         };
         _mm512_storeu_si512(result.as_mut_ptr().add(i).cast(), value);
      }
   }
   end
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn combine_avx2<const OP: u8>(result: &mut [u8], buf: &[u8]) -> usize {
   #[cfg(target_arch = "x86")]
   use std::arch::x86::*;
   #[cfg(target_arch = "x86_64")]
   use std::arch::x86_64::*;

   let end = result.len().min(buf.len()) & !31;
   for i in (0..end).step_by(32) {
      // SAFETY: Each unaligned load/store accesses 32 bytes within both slices.
      unsafe {
         let b = _mm256_loadu_si256(buf.as_ptr().add(i).cast());
         let value = if OP == BIT_NOT {
            _mm256_xor_si256(b, _mm256_set1_epi8(-1))
         } else {
            let a = _mm256_loadu_si256(result.as_ptr().add(i).cast());
            match OP {
               BIT_AND => _mm256_and_si256(a, b),
               BIT_OR => _mm256_or_si256(a, b),
               BIT_XOR => _mm256_xor_si256(a, b),
               _ => unreachable!(),
            }
         };
         _mm256_storeu_si256(result.as_mut_ptr().add(i).cast(), value);
      }
   }
   end
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn combine_neon<const OP: u8>(result: &mut [u8], buf: &[u8]) -> usize {
   use std::arch::aarch64::*;

   let end = result.len().min(buf.len()) & !15;
   for i in (0..end).step_by(16) {
      // SAFETY: Each unaligned load/store accesses 16 bytes within both slices.
      unsafe {
         let b = vld1q_u8(buf.as_ptr().add(i));
         let value = if OP == BIT_NOT {
            vmvnq_u8(b)
         } else {
            let a = vld1q_u8(result.as_ptr().add(i));
            match OP {
               BIT_AND => vandq_u8(a, b),
               BIT_OR => vorrq_u8(a, b),
               BIT_XOR => veorq_u8(a, b),
               _ => unreachable!(),
            }
         };
         vst1q_u8(result.as_mut_ptr().add(i), value);
      }
   }
   end
}

#[inline]
fn normalize_bit_index(idx: i64, bit_len: usize) -> usize {
   if idx < 0 {
      (bit_len as i64 + idx).max(0) as usize
   } else {
      (idx as usize).min(bit_len - 1)
   }
}
