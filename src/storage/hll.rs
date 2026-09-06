use crate::storage::store::Store;
use crate::storage::value::{FyroDB, SmallStr, StoreValue};

const HLL_REGISTERS: usize = 16384;

#[derive(Clone)]
pub struct HllData {
    pub registers: Vec<u8>,
}

impl Default for HllData {
    fn default() -> Self {
        Self::new()
    }
}

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

    pub fn count(&self) -> u64 {
        let mut sum = 0.0f64;
        let mut zeros = 0u32;
        for &reg in &self.registers {
            sum += 1.0 / (1u64 << reg) as f64;
            if reg == 0 {
                zeros += 1;
            }
        }
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

#[inline]
fn leading_zeros_after(mut w: u64) -> u8 {
    if w == 0 {
        return 50;
    }
    let mut count = 0u8;
    while w & 1 == 0 && count < 50 {
        count += 1;
        w >>= 1;
    }
    count
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
                val.value = FyroDB::String(SmallStr::from_string(encoded));
                val.expires_ms = 0;
                return Ok(changed);
            }
            match val.value.as_string_mut() {
                Some(s) => {
                    let mut hll = decode_hll(s.as_bytes());
                    let mut changed = false;
                    for elem in elements {
                        if hll.add(elem.as_bytes()) {
                            changed = true;
                        }
                    }
                    if changed {
                        *s = SmallStr::from_string(encode_hll(&hll));
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
                self.data
                    .insert_str(key, StoreValue::string(encoded));
                Ok(changed)
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
        self.data
            .insert(dest.to_string(), StoreValue::string(encoded));
        Ok(())
    }
}

fn encode_hll(hll: &HllData) -> String {
    let mut out = Vec::with_capacity(4 + HLL_REGISTERS);
    out.extend_from_slice(b"HLL\x01");
    out.extend_from_slice(&hll.registers);
    unsafe { String::from_utf8_unchecked(out) }
}

fn decode_hll(data: &[u8]) -> HllData {
    if data.len() >= 4 + HLL_REGISTERS && &data[..4] == b"HLL\x01" {
        HllData {
            registers: data[4..4 + HLL_REGISTERS].to_vec(),
        }
    } else {
        HllData::new()
    }
}
