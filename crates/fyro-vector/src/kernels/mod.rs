#[cfg(target_arch = "x86_64")]
pub mod avx512;
pub mod convert;
#[cfg(target_arch = "aarch64")]
pub mod neon;

/// Dot product and ‖b‖² in one pass over both slices.
#[derive(Clone, Copy)]
pub struct DotSelf {
   pub dot: f32,
   pub b_norm_sq: f32,
}

type DotFn = unsafe fn(&[f32], &[f32]) -> f32;
type L2Fn = unsafe fn(&[f32], &[f32]) -> f32;
type DotSelfFn = unsafe fn(&[f32], &[f32]) -> DotSelf;

struct Kernels {
   dot: DotFn,
   l2: L2Fn,
   dot_self: DotSelfFn,
}

#[cold]
fn select() -> Kernels {
   #[cfg(target_arch = "x86_64")]
   if avx512_usable() {
      return Kernels {
         dot: dot_avx512,
         l2: l2_avx512,
         dot_self: dot_self_avx2,
      };
   }
   #[cfg(target_arch = "aarch64")]
   return Kernels {
      dot: dot_neon,
      l2: l2_neon,
      dot_self: dot_self_scalar,
   };
   #[cfg(target_arch = "x86_64")]
   if std::arch::is_x86_feature_detected!("avx2") && std::arch::is_x86_feature_detected!("fma") {
      return Kernels {
         dot: dot_avx2,
         l2: l2_avx2,
         dot_self: dot_self_avx2,
      };
   }
   Kernels {
      dot: dot_scalar,
      l2: l2_scalar,
      dot_self: dot_self_scalar,
   }
}

static KERNELS: std::sync::LazyLock<Kernels> = std::sync::LazyLock::new(select);

/// Sum of a·b elementwise.
#[inline(always)]
pub fn dot_f32(a: &[f32], b: &[f32]) -> f32 {
   // SAFETY: the dispatcher only installs kernel pointers whose required
   // CPU features passed runtime detection at initialization.
   unsafe { (KERNELS.dot)(a, b) }
}

/// Euclidean distance (sqrt of sum of squared differences).
#[inline(always)]
pub fn l2_f32(a: &[f32], b: &[f32]) -> f32 {
   // SAFETY: as in `dot_f32`.
   unsafe { (KERNELS.l2)(a, b) }.sqrt()
}

/// Dot product plus ‖b‖² in one fused pass.
#[inline(always)]
pub fn dot_self_f32(a: &[f32], b: &[f32]) -> DotSelf {
   // SAFETY: as in `dot_f32`.
   unsafe { (KERNELS.dot_self)(a, b) }
}

/// ‖a‖²
#[inline(always)]
pub fn norm_sq_f32(a: &[f32]) -> f32 {
   dot_f32(a, a)
}

/// Squared Euclidean distance (no sqrt): ranking-equivalent to L2.
#[inline(always)]
pub fn l2sq_f32(a: &[f32], b: &[f32]) -> f32 {
   // SAFETY: as in `dot_f32`.
   unsafe { (KERNELS.l2)(a, b) }
}

/// Kernel set for monomorphized hot loops obtained via [`isa`], allowing
/// generic loops over `K` to inline kernels instead of function pointers.
pub trait Kernel: Copy + Send + Sync + 'static {
   fn dot(self, a: &[f32], b: &[f32]) -> f32;
   fn l2sq(self, a: &[f32], b: &[f32]) -> f32;
   /// Σ a·b over int8 codes (quantized traversal).
   fn dot_i8(self, a: &[i8], b: &[i8]) -> i32;
   /// Σ_j 2^j · popcount(code ∧ plane_j) (RaBitQ estimator).
   fn plane_popcount(self, code: &[u64], planes: &[u64], nplanes: usize) -> u64 {
      plane_popcount_scalar(code, planes, nplanes)
   }
}

#[inline(always)]
pub fn dot_i8_scalar(a: &[i8], b: &[i8]) -> i32 {
   a.iter().zip(b).map(|(&x, &y)| x as i32 * y as i32).sum()
}

#[inline(always)]
pub fn plane_popcount_scalar(code: &[u64], planes: &[u64], nplanes: usize) -> u64 {
   let words = code.len();
   let mut total = 0u64;
   for j in 0..nplanes {
      let plane = &planes[j * words..(j + 1) * words];
      let s: u64 = code
         .iter()
         .zip(plane)
         .map(|(c, p)| (c & p).count_ones() as u64)
         .sum();
      total += s << j;
   }
   total
}

#[derive(Clone, Copy)]
pub struct Scalar;

impl Kernel for Scalar {
   #[inline(always)]
   fn dot(self, a: &[f32], b: &[f32]) -> f32 {
      dot_scalar(a, b)
   }
   #[inline(always)]
   fn l2sq(self, a: &[f32], b: &[f32]) -> f32 {
      l2_scalar(a, b)
   }
   #[inline(always)]
   fn dot_i8(self, a: &[i8], b: &[i8]) -> i32 {
      dot_i8_scalar(a, b)
   }
}

/// AVX-512 F/BW/VNNI/VPOPCNTDQ (Ice Lake and later, Zen 4 and later).
#[cfg(target_arch = "x86_64")]
#[derive(Clone, Copy)]
pub struct Avx512(());

#[cfg(target_arch = "x86_64")]
impl Kernel for Avx512 {
   #[inline(always)]
   fn dot(self, a: &[f32], b: &[f32]) -> f32 {
      // SAFETY: an `Avx512` value exists only after runtime detection.
      unsafe { avx512::dot(a, b) }
   }
   #[inline(always)]
   fn l2sq(self, a: &[f32], b: &[f32]) -> f32 {
      // SAFETY: as above.
      unsafe { avx512::l2sq(a, b) }
   }
   #[inline(always)]
   fn dot_i8(self, a: &[i8], b: &[i8]) -> i32 {
      // SAFETY: as above.
      unsafe { avx512::dot_i8(a, b) }
   }
   #[inline(always)]
   fn plane_popcount(self, code: &[u64], planes: &[u64], nplanes: usize) -> u64 {
      // SAFETY: as above.
      unsafe { avx512::masked_popcount_sum(code, planes, nplanes) }
   }
}

#[cfg(target_arch = "x86_64")]
fn avx512_usable() -> bool {
   use std::arch::is_x86_feature_detected as has;
   has!("avx512f") && has!("avx512bw") && has!("avx512vnni") && has!("avx512vpopcntdq") && has!("avx2") && has!("fma")
        // Escape hatch for CPUs that downclock hard on 512-bit work.
        && std::env::var("FYRODB_DISABLE_AVX512").map_or(true, |v| v != "1")
}

#[cfg(target_arch = "aarch64")]
#[derive(Clone, Copy)]
pub struct Neon(());

#[cfg(target_arch = "aarch64")]
impl Kernel for Neon {
   #[inline(always)]
   fn dot(self, a: &[f32], b: &[f32]) -> f32 {
      // SAFETY: NEON is mandatory on aarch64.
      unsafe { neon::dot(a, b) }
   }
   #[inline(always)]
   fn l2sq(self, a: &[f32], b: &[f32]) -> f32 {
      // SAFETY: as above.
      unsafe { neon::l2sq(a, b) }
   }
   #[inline(always)]
   fn dot_i8(self, a: &[i8], b: &[i8]) -> i32 {
      // SAFETY: as above.
      unsafe { neon::dot_i8(a, b) }
   }
}

#[cfg(target_arch = "x86_64")]
#[derive(Clone, Copy)]
pub struct Avx2(());

#[cfg(target_arch = "x86_64")]
impl Kernel for Avx2 {
   #[inline(always)]
   fn dot(self, a: &[f32], b: &[f32]) -> f32 {
      // SAFETY: an `Avx2` value exists only after runtime detection.
      unsafe { avx2::dot(a, b) }
   }
   #[inline(always)]
   fn l2sq(self, a: &[f32], b: &[f32]) -> f32 {
      // SAFETY: as above.
      unsafe { avx2::l2(a, b) }
   }
   #[inline(always)]
   fn dot_i8(self, a: &[i8], b: &[i8]) -> i32 {
      // SAFETY: as above.
      unsafe { avx2::dot_i8(a, b) }
   }
}

/// The best kernel set this CPU supports, detected once.
#[derive(Clone, Copy)]
pub enum Isa {
   Scalar(Scalar),
   #[cfg(target_arch = "x86_64")]
   Avx2(Avx2),
   #[cfg(target_arch = "x86_64")]
   Avx512(Avx512),
   #[cfg(target_arch = "aarch64")]
   Neon(Neon),
}

impl Isa {
   pub fn name(self) -> &'static str {
      match self {
         Isa::Scalar(_) => "scalar",
         #[cfg(target_arch = "x86_64")]
         Isa::Avx2(_) => "avx2",
         #[cfg(target_arch = "x86_64")]
         Isa::Avx512(_) => "avx512",
         #[cfg(target_arch = "aarch64")]
         Isa::Neon(_) => "neon",
      }
   }
}

static ISA: std::sync::LazyLock<Isa> = std::sync::LazyLock::new(|| {
   #[cfg(target_arch = "x86_64")]
   {
      if avx512_usable() {
         return Isa::Avx512(Avx512(()));
      }
      if std::arch::is_x86_feature_detected!("avx2") && std::arch::is_x86_feature_detected!("fma") {
         return Isa::Avx2(Avx2(()));
      }
   }
   #[cfg(target_arch = "aarch64")]
   return Isa::Neon(Neon(()));
   #[allow(unreachable_code)]
   Isa::Scalar(Scalar)
});

#[inline(always)]
pub fn isa() -> Isa {
   *ISA
}

// ── scalar fallback ─────────────────────────────────────────────────────

fn dot_scalar(a: &[f32], b: &[f32]) -> f32 {
   let (mut s0, mut s1) = (0f32, 0f32);
   let chunks = a.len() / 2 * 2;
   let mut i = 0;
   while i < chunks {
      s0 = a[i].mul_add(b[i], s0);
      s1 = a[i + 1].mul_add(b[i + 1], s1);
      i += 2;
   }
   while i < a.len() {
      s0 = a[i].mul_add(b[i], s0);
      i += 1;
   }
   s0 + s1
}

fn l2_scalar(a: &[f32], b: &[f32]) -> f32 {
   let (mut s0, mut s1) = (0f32, 0f32);
   let chunks = a.len() / 2 * 2;
   let mut i = 0;
   while i < chunks {
      let d0 = a[i] - b[i];
      let d1 = a[i + 1] - b[i + 1];
      s0 = d0.mul_add(d0, s0);
      s1 = d1.mul_add(d1, s1);
      i += 2;
   }
   while i < a.len() {
      let d = a[i] - b[i];
      s0 = d.mul_add(d, s0);
      i += 1;
   }
   s0 + s1
}

fn dot_self_scalar(a: &[f32], b: &[f32]) -> DotSelf {
   let (mut s0, mut s1, mut t0, mut t1) = (0f32, 0f32, 0f32, 0f32);
   let chunks = a.len() / 2 * 2;
   let mut i = 0;
   while i < chunks {
      s0 = a[i].mul_add(b[i], s0);
      s1 = a[i + 1].mul_add(b[i + 1], s1);
      t0 = b[i].mul_add(b[i], t0);
      t1 = b[i + 1].mul_add(b[i + 1], t1);
      i += 2;
   }
   while i < a.len() {
      s0 = a[i].mul_add(b[i], s0);
      t0 = b[i].mul_add(b[i], t0);
      i += 1;
   }
   DotSelf {
      dot: s0 + s1,
      b_norm_sq: t0 + t1,
   }
}

// ── AVX2 + FMA ──────────────────────────────────────────────────────────

#[cfg(target_arch = "x86_64")]
mod avx2 {
   use super::DotSelf;
   use std::arch::x86_64::*;

   #[inline(always)]
   unsafe fn hsum(v: __m256) -> f32 {
      unsafe {
         let lo = _mm256_extractf128_ps(v, 1);
         let hi = _mm256_castps256_ps128(v);
         let s = _mm_add_ps(lo, hi);
         let s = _mm_add_ps(s, _mm_movehl_ps(s, s));
         let s = _mm_add_ss(s, _mm_shuffle_ps(s, s, 1));
         _mm_cvtss_f32(s)
      }
   }

   #[inline]
   #[target_feature(enable = "avx2,fma")]
   pub unsafe fn dot(a: &[f32], b: &[f32]) -> f32 {
      unsafe {
         let n = a.len();
         let body = n / 32 * 32;
         let mut i = 0usize;
         let mut acc = [_mm256_setzero_ps(); 4];
         while i < body {
            let av = a.as_ptr().add(i);
            let bv = b.as_ptr().add(i);
            for (k, a) in acc.iter_mut().enumerate() {
               *a = _mm256_fmadd_ps(
                  _mm256_loadu_ps(av.add(k * 8)),
                  _mm256_loadu_ps(bv.add(k * 8)),
                  *a,
               );
            }
            i += 32;
         }
         let vec_end = n / 8 * 8;
         while i < vec_end {
            acc[0] = _mm256_fmadd_ps(
               _mm256_loadu_ps(a.as_ptr().add(i)),
               _mm256_loadu_ps(b.as_ptr().add(i)),
               acc[0],
            );
            i += 8;
         }
         let mut sum = hsum(_mm256_add_ps(
            _mm256_add_ps(acc[0], acc[1]),
            _mm256_add_ps(acc[2], acc[3]),
         ));
         while i < n {
            sum = a[i].mul_add(b[i], sum);
            i += 1;
         }
         sum
      }
   }

   #[inline]
   #[target_feature(enable = "avx2,fma")]
   pub unsafe fn l2(a: &[f32], b: &[f32]) -> f32 {
      unsafe {
         let n = a.len();
         let body = n / 32 * 32;
         let mut i = 0usize;
         let mut acc = [_mm256_setzero_ps(); 4];
         while i < body {
            let av = a.as_ptr().add(i);
            let bv = b.as_ptr().add(i);
            for (k, acc_k) in acc.iter_mut().enumerate() {
               let d = _mm256_sub_ps(
                  _mm256_loadu_ps(av.add(k * 8)),
                  _mm256_loadu_ps(bv.add(k * 8)),
               );
               *acc_k = _mm256_fmadd_ps(d, d, *acc_k);
            }
            i += 32;
         }
         let vec_end = n / 8 * 8;
         while i < vec_end {
            let d = _mm256_sub_ps(
               _mm256_loadu_ps(a.as_ptr().add(i)),
               _mm256_loadu_ps(b.as_ptr().add(i)),
            );
            acc[0] = _mm256_fmadd_ps(d, d, acc[0]);
            i += 8;
         }
         let mut sum = hsum(_mm256_add_ps(
            _mm256_add_ps(acc[0], acc[1]),
            _mm256_add_ps(acc[2], acc[3]),
         ));
         while i < n {
            let d = a[i] - b[i];
            sum = d.mul_add(d, sum);
            i += 1;
         }
         sum
      }
   }

   /// Σ a·b over int8 codes: sign-extend 16 bytes to i16, `vpmaddwd`
   /// pairs into i32 lanes.
   #[inline]
   #[target_feature(enable = "avx2,fma")]
   pub unsafe fn dot_i8(a: &[i8], b: &[i8]) -> i32 {
      unsafe {
         let n = a.len().min(b.len());
         let (pa, pb) = (a.as_ptr(), b.as_ptr());
         let mut acc0 = _mm256_setzero_si256();
         let mut acc1 = _mm256_setzero_si256();
         let mut i = 0usize;
         while i + 32 <= n {
            let a0 = _mm256_cvtepi8_epi16(_mm_loadu_si128(pa.add(i) as *const __m128i));
            let b0 = _mm256_cvtepi8_epi16(_mm_loadu_si128(pb.add(i) as *const __m128i));
            let a1 = _mm256_cvtepi8_epi16(_mm_loadu_si128(pa.add(i + 16) as *const __m128i));
            let b1 = _mm256_cvtepi8_epi16(_mm_loadu_si128(pb.add(i + 16) as *const __m128i));
            acc0 = _mm256_add_epi32(acc0, _mm256_madd_epi16(a0, b0));
            acc1 = _mm256_add_epi32(acc1, _mm256_madd_epi16(a1, b1));
            i += 32;
         }
         let acc = _mm256_add_epi32(acc0, acc1);
         let s = _mm_add_epi32(
            _mm256_castsi256_si128(acc),
            _mm256_extracti128_si256(acc, 1),
         );
         let s = _mm_add_epi32(s, _mm_shuffle_epi32(s, 0b01_00_11_10));
         let s = _mm_add_epi32(s, _mm_shuffle_epi32(s, 0b10_11_00_01));
         let mut sum = _mm_cvtsi128_si32(s);
         while i < n {
            sum += *pa.add(i) as i32 * *pb.add(i) as i32;
            i += 1;
         }
         sum
      }
   }

   /// One pass accumulating dot(a,b) and ‖b‖². `b` is the stored vector.
   #[inline]
   #[target_feature(enable = "avx2,fma")]
   pub unsafe fn dot_self(a: &[f32], b: &[f32]) -> DotSelf {
      unsafe {
         let n = a.len();
         let body = n / 16 * 16;
         let mut i = 0usize;
         let mut acc = [_mm256_setzero_ps(); 4];
         let mut bacc = [_mm256_setzero_ps(); 4];
         while i < body {
            let av = a.as_ptr().add(i);
            let bv = b.as_ptr().add(i);
            for k in 0..2 {
               let bx = _mm256_loadu_ps(bv.add(k * 8));
               acc[k] = _mm256_fmadd_ps(_mm256_loadu_ps(av.add(k * 8)), bx, acc[k]);
               bacc[k] = _mm256_fmadd_ps(bx, bx, bacc[k]);
            }
            i += 16;
         }
         let vec_end = n / 8 * 8;
         while i < vec_end {
            let bx = _mm256_loadu_ps(b.as_ptr().add(i));
            acc[0] = _mm256_fmadd_ps(_mm256_loadu_ps(a.as_ptr().add(i)), bx, acc[0]);
            bacc[0] = _mm256_fmadd_ps(bx, bx, bacc[0]);
            i += 8;
         }
         let mut dot = hsum(_mm256_add_ps(acc[0], acc[1]));
         let mut bn = hsum(_mm256_add_ps(bacc[0], bacc[1]));
         while i < n {
            dot = a[i].mul_add(b[i], dot);
            bn = b[i].mul_add(b[i], bn);
            i += 1;
         }
         DotSelf { dot, b_norm_sq: bn }
      }
   }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn dot_avx2(a: &[f32], b: &[f32]) -> f32 {
   unsafe { avx2::dot(a, b) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn l2_avx2(a: &[f32], b: &[f32]) -> f32 {
   unsafe { avx2::l2(a, b) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn dot_self_avx2(a: &[f32], b: &[f32]) -> DotSelf {
   unsafe { avx2::dot_self(a, b) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn dot_avx512(a: &[f32], b: &[f32]) -> f32 {
   unsafe { avx512::dot(a, b) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn l2_avx512(a: &[f32], b: &[f32]) -> f32 {
   unsafe { avx512::l2sq(a, b) }
}

#[cfg(target_arch = "aarch64")]
unsafe fn dot_neon(a: &[f32], b: &[f32]) -> f32 {
   unsafe { neon::dot(a, b) }
}

#[cfg(target_arch = "aarch64")]
unsafe fn l2_neon(a: &[f32], b: &[f32]) -> f32 {
   unsafe { neon::l2sq(a, b) }
}

#[cfg(test)]
mod tests {
   use super::*;

   fn vecf(seed: u64, n: usize) -> Vec<f32> {
      let mut s = seed;
      (0..n)
         .map(|_| {
            s = s
               .wrapping_mul(6364136223846793005)
               .wrapping_add(1442695040888963407);
            ((s >> 40) as f32 / (1u32 << 24) as f32) * 2.0 - 1.0
         })
         .collect()
   }

   fn assert_close(a: f32, b: f32, what: &str) {
      let tol = 1e-2f32.max(a.abs().max(b.abs()) * 1e-3);
      assert!((a - b).abs() < tol, "{what}: {a} vs {b}");
   }

   #[test]
   #[cfg(target_arch = "x86_64")]
   fn dispatch_paths_agree() {
      for dims in [1usize, 7, 8, 9, 15, 16, 31, 64, 640, 768, 777, 1536] {
         let a = vecf(dims as u64 + 1, dims);
         let b = vecf(dims as u64 + 2, dims);
         assert_close(unsafe { dot_avx2(&a, &b) }, dot_scalar(&a, &b), "dot");
         assert_close(unsafe { l2_avx2(&a, &b) }, l2_scalar(&a, &b), "l2");
         let dsv = unsafe { dot_self_avx2(&a, &b) };
         let dss = dot_self_scalar(&a, &b);
         assert_close(dsv.dot, dss.dot, "dot_self.dot");
         assert_close(dsv.b_norm_sq, dss.b_norm_sq, "dot_self.b");
      }
   }

   #[test]
   #[cfg(target_arch = "x86_64")]
   fn int8_and_avx512_paths_agree() {
      for dims in [1usize, 15, 16, 31, 32, 33, 64, 127, 768, 1536] {
         let a: Vec<i8> = (0..dims)
            .map(|i| ((i * 37 + 11) % 255) as i32 as u8 as i8)
            .map(|x| x.max(-127))
            .collect();
         let b: Vec<i8> = (0..dims)
            .map(|i| ((i * 91 + 3) % 255) as i32 as u8 as i8)
            .map(|x| x.max(-127))
            .collect();
         let want = dot_i8_scalar(&a, &b);
         assert_eq!(
            unsafe { avx2::dot_i8(&a, &b) },
            want,
            "avx2 dot_i8 dims={dims}"
         );
         if avx512_usable() {
            assert_eq!(
               unsafe { avx512::dot_i8(&a, &b) },
               want,
               "avx512 dot_i8 dims={dims}"
            );
            let fa = vecf(dims as u64 + 1, dims);
            let fb = vecf(dims as u64 + 2, dims);
            assert_close(
               unsafe { avx512::dot(&fa, &fb) },
               dot_scalar(&fa, &fb),
               "avx512 dot",
            );
            assert_close(
               unsafe { avx512::l2sq(&fa, &fb) },
               l2_scalar(&fa, &fb),
               "avx512 l2",
            );
            let words = dims.div_ceil(64);
            let code: Vec<u64> = (0..words as u64)
               .map(|i| i.wrapping_mul(0x9E3779B97F4A7C15))
               .collect();
            let planes: Vec<u64> = (0..words as u64 * 4)
               .map(|i| i.wrapping_mul(0xBF58476D1CE4E5B9))
               .collect();
            assert_eq!(
               unsafe { avx512::masked_popcount_sum(&code, &planes, 4) },
               plane_popcount_scalar(&code, &planes, 4)
            );
         }
      }
   }

   #[test]
   fn public_kernels_match_reference() {
      let a = vecf(5, 100);
      let b = vecf(6, 100);
      let mut ref_dot = 0f64;
      let mut ref_l2 = 0f64;
      for i in 0..a.len() {
         ref_dot += a[i] as f64 * b[i] as f64;
         ref_l2 += (a[i] as f64 - b[i] as f64).powi(2);
      }
      assert!((dot_f32(&a, &b) as f64 - ref_dot).abs() < 1e-3);
      assert!((l2_f32(&a, &b) - ref_l2.sqrt() as f32).abs() < 1e-2);
      let ds = dot_self_f32(&a, &b);
      let ref_bn: f64 = b.iter().map(|x| (*x as f64).powi(2)).sum();
      assert!((ds.dot as f64 - ref_dot).abs() < 1e-3);
      assert!((ds.b_norm_sq as f64 - ref_bn).abs() < 1e-3);
   }
}
