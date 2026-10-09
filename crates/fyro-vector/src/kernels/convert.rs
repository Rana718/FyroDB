#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ElemType {
    Float32,
    Float16,
    BFloat16,
    Int8,
    UInt8,
}

impl ElemType {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s.to_ascii_uppercase().as_str() {
            "FLOAT32" => Self::Float32,
            "FLOAT16" => Self::Float16,
            "BFLOAT16" => Self::BFloat16,
            "INT8" => Self::Int8,
            "UINT8" => Self::UInt8,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Float32 => "FLOAT32",
            Self::Float16 => "FLOAT16",
            Self::BFloat16 => "BFLOAT16",
            Self::Int8 => "INT8",
            Self::UInt8 => "UINT8",
        }
    }

    /// Bytes per element in a blob of this type.
    pub fn width(self) -> usize {
        match self {
            Self::Float32 => 4,
            Self::Float16 | Self::BFloat16 => 2,
            Self::Int8 | Self::UInt8 => 1,
        }
    }

    /// Decode a little-endian blob of `dims` elements; None on a length
    /// mismatch.
    pub fn decode(self, bytes: &[u8], dims: usize) -> Option<Vec<f32>> {
        if bytes.len() != dims * self.width() {
            return None;
        }
        let mut out = Vec::with_capacity(dims);
        match self {
            Self::Float32 => out.extend(bytes.as_chunks::<4>().0.iter().map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))),
            Self::Float16 => decode_f16(bytes, &mut out),
            Self::BFloat16 => {
                out.extend(bytes.as_chunks::<2>().0.iter().map(|c| f32::from_bits((u16::from_le_bytes([c[0], c[1]]) as u32) << 16)))
            }
            Self::Int8 => out.extend(bytes.iter().map(|&b| b as i8 as f32)),
            Self::UInt8 => out.extend(bytes.iter().map(|&b| b as f32)),
        }
        Some(out)
    }
}

fn decode_f16(bytes: &[u8], out: &mut Vec<f32>) {
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("f16c") && std::arch::is_x86_feature_detected!("avx") {
        // SAFETY: features checked above.
        unsafe { decode_f16_f16c(bytes, out) };
        return;
    }
    out.extend(bytes.as_chunks::<2>().0.iter().map(|c| f16_to_f32(u16::from_le_bytes([c[0], c[1]]))));
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "f16c,avx")]
unsafe fn decode_f16_f16c(bytes: &[u8], out: &mut Vec<f32>) {
    use std::arch::x86_64::*;
    let n = bytes.len() / 2;
    let mut i = 0usize;
    unsafe {
        while i + 8 <= n {
            let h = _mm_loadu_si128(bytes.as_ptr().add(i * 2) as *const __m128i);
            let f = _mm256_cvtph_ps(h);
            let mut tmp = [0f32; 8];
            _mm256_storeu_ps(tmp.as_mut_ptr(), f);
            out.extend_from_slice(&tmp);
            i += 8;
        }
    }
    out.extend(bytes[i * 2..].as_chunks::<2>().0.iter().map(|c| f16_to_f32(u16::from_le_bytes([c[0], c[1]]))));
}

/// IEEE 754 binary16 → binary32 (subnormals, infinities and NaN included).
pub fn f16_to_f32(h: u16) -> f32 {
    let sign = ((h & 0x8000) as u32) << 16;
    let exp = ((h >> 10) & 0x1f) as u32;
    let frac = (h & 0x3ff) as u32;
    let bits = match exp {
        0 if frac == 0 => sign,
        0 => {
            // Subnormal: renormalize.
            let mut e = 127 - 15 + 1;
            let mut f = frac;
            while f & 0x400 == 0 {
                f <<= 1;
                e -= 1;
            }
            sign | (e << 23) | ((f & 0x3ff) << 13)
        }
        0x1f => sign | 0x7f80_0000 | (frac << 13),
        _ => sign | ((exp + 127 - 15) << 23) | (frac << 13),
    };
    f32::from_bits(bits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn f16_decodes_known_values() {
        assert_eq!(f16_to_f32(0x3c00), 1.0);
        assert_eq!(f16_to_f32(0xc000), -2.0);
        assert_eq!(f16_to_f32(0x7bff), 65504.0);
        assert_eq!(f16_to_f32(0x0001), 5.960_464_5e-8);
        assert!(f16_to_f32(0x7e00).is_nan());
        let blob: Vec<u8> = [0x3c00u16, 0xc000, 0x3800, 0, 0x3c00, 0x3c00, 0x3c00, 0x3c00, 0x4000]
            .iter()
            .flat_map(|h| h.to_le_bytes())
            .collect();
        assert_eq!(
            ElemType::Float16.decode(&blob, 9).unwrap(),
            vec![1.0, -2.0, 0.5, 0.0, 1.0, 1.0, 1.0, 1.0, 2.0]
        );
    }

    #[test]
    fn other_types_decode() {
        let bf: Vec<u8> = [0x3f80u16, 0xc040].iter().flat_map(|h| h.to_le_bytes()).collect();
        assert_eq!(ElemType::BFloat16.decode(&bf, 2).unwrap(), vec![1.0, -3.0]);
        assert_eq!(ElemType::Int8.decode(&[0xff, 0x05], 2).unwrap(), vec![-1.0, 5.0]);
        assert_eq!(ElemType::UInt8.decode(&[0xff, 0x05], 2).unwrap(), vec![255.0, 5.0]);
        assert!(ElemType::Float32.decode(&[0; 7], 2).is_none());
    }
}
