use crate::DistanceMetric;
use crate::kernels as simd;

#[inline(always)]
pub fn cosine_f32(a: &[f32], b: &[f32]) -> f32 {
   let ds = simd::dot_self_f32(a, b);
   let na = simd::norm_sq_f32(a);
   1.0 - ds.dot / (na.sqrt() * ds.b_norm_sq.sqrt())
}

#[inline(always)]
pub fn cosine_bytes(a: &[u8], b: &[u8]) -> f32 {
   cosine_f32(
      bytemuck::cast_slice::<u8, f32>(a),
      bytemuck::cast_slice::<u8, f32>(b),
   )
}

#[inline(always)]
pub fn l2_f32(a: &[f32], b: &[f32]) -> f32 {
   simd::l2_f32(a, b)
}

#[inline(always)]
pub fn l2_bytes(a: &[u8], b: &[u8]) -> f32 {
   l2_f32(
      bytemuck::cast_slice::<u8, f32>(a),
      bytemuck::cast_slice::<u8, f32>(b),
   )
}

#[inline(always)]
pub fn ip_f32(a: &[f32], b: &[f32]) -> f32 {
   -simd::dot_f32(a, b)
}

pub fn distance(metric: DistanceMetric, a: &[f32], b: &[f32]) -> f32 {
   match metric {
      DistanceMetric::L2 => l2_f32(a, b),
      DistanceMetric::Cosine => cosine_f32(a, b),
      DistanceMetric::Ip => ip_f32(a, b),
   }
}

pub fn distance_bytes(metric: DistanceMetric, a: &[u8], b: &[u8]) -> f32 {
   let av = bytemuck::cast_slice::<u8, f32>(a);
   let bv = bytemuck::cast_slice::<u8, f32>(b);
   match metric {
      DistanceMetric::L2 => l2_f32(av, bv),
      DistanceMetric::Cosine => cosine_f32(av, bv),
      DistanceMetric::Ip => ip_f32(av, bv),
   }
}

/// Query vector distance calculator with precomputed query norm per search,
/// avoiding redundant norm calculations on cosine distance paths.
#[derive(Clone, Copy)]
pub struct QueryDist {
   metric: DistanceMetric,
   qnorm: f32,
}

impl QueryDist {
   pub fn new(metric: DistanceMetric, query: &[f32]) -> Self {
      let qnorm = match metric {
         DistanceMetric::Cosine => simd::norm_sq_f32(query).sqrt().max(1e-10),
         _ => 1.0,
      };
      Self { metric, qnorm }
   }

   #[inline(always)]
   pub fn distance(&self, query: &[f32], doc: &[f32]) -> f32 {
      match self.metric {
         DistanceMetric::L2 => simd::l2_f32(query, doc),
         DistanceMetric::Cosine => {
            let ds = simd::dot_self_f32(query, doc);
            1.0 - ds.dot / (self.qnorm * ds.b_norm_sq.sqrt())
         }
         DistanceMetric::Ip => -simd::dot_f32(query, doc),
      }
   }

   #[inline(always)]
   pub fn distance_bytes(&self, query: &[u8], doc: &[u8]) -> f32 {
      self.distance(
         bytemuck::cast_slice::<u8, f32>(query),
         bytemuck::cast_slice::<u8, f32>(doc),
      )
   }
}

#[cfg(test)]
mod tests {
   use super::*;

   #[test]
   fn cosine_bounds() {
      let a = vec![1.0f32, 0.0];
      let b = vec![1.0f32, 0.0];
      assert!(cosine_f32(&a, &b).abs() < 1e-6);
      let c = vec![0.0f32, 1.0];
      assert!((cosine_f32(&a, &c) - 1.0).abs() < 1e-6);
   }

   #[test]
   fn l2_and_ip() {
      let a = vec![0.0f32, 3.0];
      let b = vec![4.0f32, 0.0];
      assert!((l2_f32(&a, &b) - 5.0).abs() < 1e-5);
      let x = vec![2.0f32, 3.0];
      let y = vec![4.0f32, 1.0];
      assert!((ip_f32(&x, &y) + 11.0).abs() < 1e-5);
   }
}
