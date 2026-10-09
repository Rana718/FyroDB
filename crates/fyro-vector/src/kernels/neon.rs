use std::arch::aarch64::*;

#[inline]
pub unsafe fn dot(a: &[f32], b: &[f32]) -> f32 {
    unsafe {
        let n = a.len().min(b.len());
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        let mut acc = [vdupq_n_f32(0.0); 4];
        let mut i = 0usize;
        while i + 16 <= n {
            for (k, acc_k) in acc.iter_mut().enumerate() {
                *acc_k = vfmaq_f32(*acc_k, vld1q_f32(pa.add(i + k * 4)), vld1q_f32(pb.add(i + k * 4)));
            }
            i += 16;
        }
        let mut sum = vaddvq_f32(vaddq_f32(vaddq_f32(acc[0], acc[1]), vaddq_f32(acc[2], acc[3])));
        while i < n {
            sum += *pa.add(i) * *pb.add(i);
            i += 1;
        }
        sum
    }
}

#[inline]
pub unsafe fn l2sq(a: &[f32], b: &[f32]) -> f32 {
    unsafe {
        let n = a.len().min(b.len());
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        let mut acc = [vdupq_n_f32(0.0); 4];
        let mut i = 0usize;
        while i + 16 <= n {
            for (k, acc_k) in acc.iter_mut().enumerate() {
                let d = vsubq_f32(vld1q_f32(pa.add(i + k * 4)), vld1q_f32(pb.add(i + k * 4)));
                *acc_k = vfmaq_f32(*acc_k, d, d);
            }
            i += 16;
        }
        let mut sum = vaddvq_f32(vaddq_f32(vaddq_f32(acc[0], acc[1]), vaddq_f32(acc[2], acc[3])));
        while i < n {
            let d = *pa.add(i) - *pb.add(i);
            sum += d * d;
            i += 1;
        }
        sum
    }
}

/// Σ a·b over int8 codes: widening multiply to i16, pairwise add to i32.
#[inline]
pub unsafe fn dot_i8(a: &[i8], b: &[i8]) -> i32 {
    unsafe {
        let n = a.len().min(b.len());
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        let mut acc = vdupq_n_s32(0);
        let mut i = 0usize;
        while i + 16 <= n {
            let va = vld1q_s8(pa.add(i));
            let vb = vld1q_s8(pb.add(i));
            let lo = vmull_s8(vget_low_s8(va), vget_low_s8(vb));
            let hi = vmull_s8(vget_high_s8(va), vget_high_s8(vb));
            acc = vpadalq_s16(acc, lo);
            acc = vpadalq_s16(acc, hi);
            i += 16;
        }
        let mut sum = vaddvq_s32(acc);
        while i < n {
            sum += *pa.add(i) as i32 * *pb.add(i) as i32;
            i += 1;
        }
        sum
    }
}
