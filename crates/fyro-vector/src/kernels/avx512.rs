use std::arch::x86_64::*;

/// # Safety
/// Caller must ensure AVX-512F is available.
#[inline]
#[target_feature(enable = "avx512f")]
pub unsafe fn dot(a: &[f32], b: &[f32]) -> f32 {
    unsafe {
        let n = a.len().min(b.len());
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        let mut acc = [_mm512_setzero_ps(); 4];
        let mut i = 0usize;
        while i + 64 <= n {
            for (k, acc_k) in acc.iter_mut().enumerate() {
                *acc_k = _mm512_fmadd_ps(_mm512_loadu_ps(pa.add(i + k * 16)), _mm512_loadu_ps(pb.add(i + k * 16)), *acc_k);
            }
            i += 64;
        }
        while i + 16 <= n {
            acc[0] = _mm512_fmadd_ps(_mm512_loadu_ps(pa.add(i)), _mm512_loadu_ps(pb.add(i)), acc[0]);
            i += 16;
        }
        if i < n {
            let m: __mmask16 = (1u32 << (n - i)).wrapping_sub(1) as __mmask16;
            acc[1] = _mm512_fmadd_ps(_mm512_maskz_loadu_ps(m, pa.add(i)), _mm512_maskz_loadu_ps(m, pb.add(i)), acc[1]);
        }
        _mm512_reduce_add_ps(_mm512_add_ps(_mm512_add_ps(acc[0], acc[1]), _mm512_add_ps(acc[2], acc[3])))
    }
}

/// # Safety
/// Caller must ensure AVX-512F is available.
#[inline]
#[target_feature(enable = "avx512f")]
pub unsafe fn l2sq(a: &[f32], b: &[f32]) -> f32 {
    unsafe {
        let n = a.len().min(b.len());
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        let mut acc = [_mm512_setzero_ps(); 4];
        let mut i = 0usize;
        while i + 64 <= n {
            for (k, acc_k) in acc.iter_mut().enumerate() {
                let d = _mm512_sub_ps(_mm512_loadu_ps(pa.add(i + k * 16)), _mm512_loadu_ps(pb.add(i + k * 16)));
                *acc_k = _mm512_fmadd_ps(d, d, *acc_k);
            }
            i += 64;
        }
        while i + 16 <= n {
            let d = _mm512_sub_ps(_mm512_loadu_ps(pa.add(i)), _mm512_loadu_ps(pb.add(i)));
            acc[0] = _mm512_fmadd_ps(d, d, acc[0]);
            i += 16;
        }
        if i < n {
            let m: __mmask16 = (1u32 << (n - i)).wrapping_sub(1) as __mmask16;
            let d = _mm512_sub_ps(_mm512_maskz_loadu_ps(m, pa.add(i)), _mm512_maskz_loadu_ps(m, pb.add(i)));
            acc[1] = _mm512_fmadd_ps(d, d, acc[1]);
        }
        _mm512_reduce_add_ps(_mm512_add_ps(_mm512_add_ps(acc[0], acc[1]), _mm512_add_ps(acc[2], acc[3])))
    }
}

/// # Safety
/// Caller must ensure AVX-512F, BW, and VNNI are available.
#[inline]
#[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
pub unsafe fn dot_i8(a: &[i8], b: &[i8]) -> i32 {
    unsafe {
        let n = a.len().min(b.len());
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        let mut acc0 = _mm512_setzero_si512();
        let mut acc1 = _mm512_setzero_si512();
        let mut i = 0usize;
        while i + 64 <= n {
            let a0 = _mm512_cvtepi8_epi16(_mm256_loadu_si256(pa.add(i) as *const __m256i));
            let b0 = _mm512_cvtepi8_epi16(_mm256_loadu_si256(pb.add(i) as *const __m256i));
            let a1 = _mm512_cvtepi8_epi16(_mm256_loadu_si256(pa.add(i + 32) as *const __m256i));
            let b1 = _mm512_cvtepi8_epi16(_mm256_loadu_si256(pb.add(i + 32) as *const __m256i));
            acc0 = _mm512_dpwssd_epi32(acc0, a0, b0);
            acc1 = _mm512_dpwssd_epi32(acc1, a1, b1);
            i += 64;
        }
        while i + 32 <= n {
            let a0 = _mm512_cvtepi8_epi16(_mm256_loadu_si256(pa.add(i) as *const __m256i));
            let b0 = _mm512_cvtepi8_epi16(_mm256_loadu_si256(pb.add(i) as *const __m256i));
            acc0 = _mm512_dpwssd_epi32(acc0, a0, b0);
            i += 32;
        }
        let mut sum = _mm512_reduce_add_epi32(_mm512_add_epi32(acc0, acc1));
        while i < n {
            sum += *pa.add(i) as i32 * *pb.add(i) as i32;
            i += 1;
        }
        sum
    }
}

/// # Safety
/// Caller must ensure AVX-512F and VPOPCNTDQ are available.
#[inline]
#[target_feature(enable = "avx512f,avx512vpopcntdq")]
pub unsafe fn masked_popcount_sum(code: &[u64], planes: &[u64], nplanes: usize) -> u64 {
    unsafe {
        let words = code.len();
        let mut total = 0u64;
        for j in 0..nplanes {
            let plane = &planes[j * words..(j + 1) * words];
            let mut acc = _mm512_setzero_si512();
            let mut i = 0usize;
            while i + 8 <= words {
                let c = _mm512_loadu_si512(code.as_ptr().add(i) as *const _);
                let p = _mm512_loadu_si512(plane.as_ptr().add(i) as *const _);
                acc = _mm512_add_epi64(acc, _mm512_popcnt_epi64(_mm512_and_si512(c, p)));
                i += 8;
            }
            let mut s = _mm512_reduce_add_epi64(acc) as u64;
            while i < words {
                s += (code[i] & plane[i]).count_ones() as u64;
                i += 1;
            }
            total += s << j;
        }
        total
    }
}
