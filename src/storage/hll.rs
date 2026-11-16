use crate::storage::store::Store;
use crate::storage::value::{FyroDB, SmallStr, StoreValue};

const HLL_REGISTERS: usize = 16384;
const HLL_HEADER: usize = 4;
const HLL_ENCODED_LEN: usize = HLL_HEADER + HLL_REGISTERS;

#[derive(Clone)]
pub struct HllData {
   pub registers: Vec<u8>,
}

impl Default for HllData {
   fn default() -> Self {
      Self::new()
   }
}

const RECIP_2_POW: [f64; 64] = {
   let mut table = [0.0f64; 64];
   let mut i = 0;
   let mut val = 1.0f64;
   while i < 64 {
      table[i] = val;
      val *= 0.5f64;
      i += 1;
   }
   table
};

impl HllData {
   pub fn new() -> Self {
      Self {
         registers: vec![0u8; HLL_REGISTERS],
      }
   }

   pub fn add(&mut self, element: &[u8]) -> bool {
      let hash = hll_hash(element);
      let idx = (hash & 0x3FFF) as usize;
      let w = hash >> 14;
      let rho = leading_zeros_after(w) + 1;
      if rho > self.registers[idx] {
         self.registers[idx] = rho;
         true
      } else {
         false
      }
   }

   /// Register byte offset and rho value to mutate raw register buffer in place
   /// without decoding the full 16 KB array into `HllData`.
   #[inline]
   fn add_target(element: &[u8]) -> (usize, u8) {
      let hash = hll_hash(element);
      let idx = (hash & 0x3FFF) as usize;
      let w = hash >> 14;
      (idx, leading_zeros_after(w) + 1)
   }

   /// Applies one element directly to a raw `HLL\x01`-prefixed register
   /// buffer (as stored in a value). Returns true if a register changed.
   #[inline]
   fn add_in_place(buf: &mut [u8], element: &[u8]) -> bool {
      let (idx, rho) = Self::add_target(element);
      let reg = &mut buf[HLL_HEADER + idx];
      if rho > *reg {
         *reg = rho;
         true
      } else {
         false
      }
   }

   pub fn count(&self) -> u64 {
      let mut sum0 = 0.0f64;
      let mut sum1 = 0.0f64;
      let mut sum2 = 0.0f64;
      let mut sum3 = 0.0f64;
      let mut zeros = 0u32;
      let chunks = self.registers.chunks_exact(4);
      for chunk in chunks {
         sum0 += RECIP_2_POW[(chunk[0] as usize).min(63)];
         sum1 += RECIP_2_POW[(chunk[1] as usize).min(63)];
         sum2 += RECIP_2_POW[(chunk[2] as usize).min(63)];
         sum3 += RECIP_2_POW[(chunk[3] as usize).min(63)];
         zeros += (chunk[0] == 0) as u32
            + (chunk[1] == 0) as u32
            + (chunk[2] == 0) as u32
            + (chunk[3] == 0) as u32;
      }
      let sum = (sum0 + sum1) + (sum2 + sum3);
      let m = HLL_REGISTERS as f64;
      let alpha = 0.7213 / (1.0 + 1.079 / m);
      let raw = alpha * m * m / sum;

      let estimate = if raw <= 2.5 * m && zeros > 0 {
         m * (m / zeros as f64).ln()
      } else {
         raw
      };

      estimate.round() as u64
   }

   pub fn merge(&mut self, other: &HllData) {
      #[cfg(target_arch = "x86_64")]
      {
         if std::is_x86_feature_detected!("avx2") {
            unsafe {
               use std::arch::x86_64::{_mm256_loadu_si256, _mm256_max_epu8, _mm256_storeu_si256};
               let dst = self.registers.as_mut_ptr();
               let src = other.registers.as_ptr();
               for i in (0..HLL_REGISTERS).step_by(32) {
                  let v1 = _mm256_loadu_si256(dst.add(i).cast());
                  let v2 = _mm256_loadu_si256(src.add(i).cast());
                  let max = _mm256_max_epu8(v1, v2);
                  _mm256_storeu_si256(dst.add(i).cast(), max);
               }
               return;
            }
         }
      }
      for i in 0..HLL_REGISTERS {
         if other.registers[i] > self.registers[i] {
            self.registers[i] = other.registers[i];
         }
      }
   }
}

fn hll_hash(data: &[u8]) -> u64 {
   let mut h: u64 = 0xcbf29ce484222325;
   for &b in data {
      h ^= b as u64;
      h = h.wrapping_mul(0x100000001b3);
   }
   h ^= h >> 33;
   h = h.wrapping_mul(0xff51afd7ed558ccd);
   h ^= h >> 33;
   h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
   h ^= h >> 33;
   h
}

#[inline(always)]
fn leading_zeros_after(w: u64) -> u8 {
   if w == 0 {
      50
   } else {
      (w.trailing_zeros() as u8).min(50)
   }
}

impl Store {
   pub fn pfadd(&self, key: &str, elements: &[&str]) -> Result<bool, &'static str> {
      let result = self.data.update_with(key, |val| {
         if val.is_expired() {
            let mut hll = HllData::new();
            let mut changed = false;
            for elem in elements {
               if hll.add(elem.as_bytes()) {
                  changed = true;
               }
            }
            let encoded = encode_hll(&hll);
            val.value = FyroDB::String(encoded);
            val.expires_ms = 0;
            return Ok(changed);
         }
         match val.value.as_string_mut() {
            Some(s) => {
               // Mutate registers in-place: avoids 16 KB alloc + re-encode on every PFADD.
               if is_hll_buffer(s.as_bytes()) {
                  let buf = s.as_bytes_mut();
                  let mut changed = false;
                  for elem in elements {
                     if HllData::add_in_place(buf, elem.as_bytes()) {
                        changed = true;
                     }
                  }
                  return Ok(changed);
               }
               // Legacy / malformed buffer: fall back to decode + re-encode.
               let mut hll = decode_hll(s.as_bytes());
               let mut changed = false;
               for elem in elements {
                  if hll.add(elem.as_bytes()) {
                     changed = true;
                  }
               }
               if changed {
                  *s = encode_hll(&hll);
               }
               Ok(changed)
            }
            None => Err("WRONGTYPE"),
         }
      });

      match result {
         Some(r) => r,
         None => {
            let mut hll = HllData::new();
            let mut changed = false;
            for elem in elements {
               if hll.add(elem.as_bytes()) {
                  changed = true;
               }
            }
            let encoded = encode_hll(&hll);
            self.data.insert_str(key, StoreValue::string_small(encoded));
            Ok(changed)
         }
      }
   }

   // A later duplicate of an element added earlier in the same run still reports 0.
   pub fn pfadd_run(&self, key: &str, elements: &[&str]) -> Result<Vec<bool>, &'static str> {
      let apply = |buf: &mut [u8]| -> Vec<bool> {
         let mut flags = Vec::with_capacity(elements.len());
         for elem in elements {
            flags.push(HllData::add_in_place(buf, elem.as_bytes()));
         }
         flags
      };

      let result = self.data.update_with(key, |val| {
         if val.is_expired() {
            let mut encoded = encode_hll(&HllData::new()).into_bytes();
            let flags = apply(&mut encoded);
            val.value = FyroDB::String(SmallStr::from_string(unsafe {
               String::from_utf8_unchecked(encoded)
            }));
            val.expires_ms = 0;
            return Ok(flags);
         }
         match val.value.as_string_mut() {
            Some(s) => {
               if is_hll_buffer(s.as_bytes()) {
                  Ok(apply(s.as_bytes_mut()))
               } else {
                  // Legacy/malformed: normalize to a fresh buffer, then apply.
                  let mut encoded = encode_hll(&decode_hll(s.as_bytes())).into_bytes();
                  let flags = apply(&mut encoded);
                  *s = SmallStr::from_string(unsafe { String::from_utf8_unchecked(encoded) });
                  Ok(flags)
               }
            }
            None => Err("WRONGTYPE"),
         }
      });

      match result {
         Some(r) => r,
         None => {
            let mut encoded = encode_hll(&HllData::new()).into_bytes();
            let flags = apply(&mut encoded);
            self
               .data
               .insert_str(key, StoreValue::string_small(SmallStr::from_bytes(encoded)));
            Ok(flags)
         }
      }
   }

   pub fn pfcount(&self, keys: &[&str]) -> Result<u64, &'static str> {
      if keys.len() == 1 {
         match self.data.get_ref(keys[0]) {
            None => Ok(0),
            Some(e) if e.is_expired() => Ok(0),
            Some(e) => match e.value.as_string() {
               Some(s) => {
                  let hll = decode_hll(s.as_bytes());
                  Ok(hll.count())
               }
               None => Err("WRONGTYPE"),
            },
         }
      } else {
         let mut merged = HllData::new();
         for &k in keys {
            match self.data.get_ref(k) {
               None => {}
               Some(e) if e.is_expired() => {}
               Some(e) => match e.value.as_string() {
                  Some(s) => {
                     let hll = decode_hll(s.as_bytes());
                     merged.merge(&hll);
                  }
                  None => return Err("WRONGTYPE"),
               },
            }
         }
         Ok(merged.count())
      }
   }

   pub fn pfmerge(&self, dest: &str, sources: &[&str]) -> Result<(), &'static str> {
      let mut merged = HllData::new();
      for &k in sources {
         match self.data.get_ref(k) {
            None => {}
            Some(e) if e.is_expired() => {}
            Some(e) => match e.value.as_string() {
               Some(s) => {
                  let hll = decode_hll(s.as_bytes());
                  merged.merge(&hll);
               }
               None => return Err("WRONGTYPE"),
            },
         }
      }
      let encoded = encode_hll(&merged);
      self
         .data
         .insert_str(dest, StoreValue::string_small(encoded));
      Ok(())
   }
}

fn encode_hll(hll: &HllData) -> SmallStr {
   let mut out = Vec::with_capacity(HLL_ENCODED_LEN);
   out.extend_from_slice(b"HLL\x01");
   out.extend_from_slice(&hll.registers);
   SmallStr::from_bytes(out)
}

/// True when `data` is a well-formed `HLL\x01` register buffer that can be
/// mutated in place (correct magic and exact length).
#[inline]
fn is_hll_buffer(data: &[u8]) -> bool {
   data.len() == HLL_ENCODED_LEN && &data[..HLL_HEADER] == b"HLL\x01"
}

fn decode_hll(data: &[u8]) -> HllData {
   if data.len() >= HLL_ENCODED_LEN && &data[..HLL_HEADER] == b"HLL\x01" {
      HllData {
         registers: data[HLL_HEADER..HLL_ENCODED_LEN].to_vec(),
      }
   } else {
      HllData::new()
   }
}
