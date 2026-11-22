//! Int8 sketch of a FLAT graph's f32 vectors, for exact scans that read less
//! memory.
//!
//! An exact scan over f32 vectors is bound by memory bandwidth: every query
//! streams 4 bytes per dimension of every vector. The sketch keeps a second,
//! int8 copy of each vector (a quarter of the bytes) together with the norm of
//! its quantization residual. A scan scores the sketch and bounds the error of
//! each estimate exactly:
//!
//!   q·v − q̂·v̂ = q·(v − v̂) + (q − q̂)·v̂,  so  |q·v − q̂·v̂| ≤ ‖q‖‖v − v̂‖ + ‖q − q̂‖‖v̂‖
//!
//! (Cauchy–Schwarz). A vector is dropped only when even its best case cannot
//! reach the current top-k; the survivors are re-scored with their f32 vectors,
//! so results equal a full f32 scan. This is the filter-and-refine scheme of
//! the VA-file (Weber, Schek and Blott, VLDB 1998).
//!
//! Row layout, in u32 words: `[scale, norm, residual norm, code norm, codes…]`.

use super::*;

/// Header words before the codes.
const HEADER: usize = 4;

/// Below this many dimensions an f32 row is a few cache lines and the sketch
/// saves too little to pay for its refine step.
pub(crate) const MIN_DIMS: usize = 64;

/// Relative slack added to every bound. It covers f32 rounding in the exact
/// kernels (blocked sums over at most a few thousand terms) and in the
/// estimate itself, both orders of magnitude below this.
const SLACK: f32 = 1e-4;

pub(crate) fn words(dims: usize) -> usize {
   HEADER + dims.div_ceil(4)
}

/// Scale and codes of `v` (symmetric int8, one scale per vector), plus the
/// residual and code norms the bounds need. `None` for non-finite input.
fn quantize(v: &[f32], codes: &mut [i8]) -> Option<(f32, f32, f32)> {
   let maxabs = v.iter().fold(0f32, |m, x| m.max(x.abs()));
   if !maxabs.is_finite() {
      return None;
   }
   let scale = if maxabs > 0.0 { maxabs / 127.0 } else { 1.0 };
   let (mut resid, mut code_sq) = (0f64, 0f64);
   for (c, &x) in codes.iter_mut().zip(v) {
      let q = (x / scale).round().clamp(-127.0, 127.0);
      *c = q as i8;
      let back = q as f64 * scale as f64;
      resid += (x as f64 - back).powi(2);
      code_sq += back * back;
   }
   // Rounded up so the bound stays a bound after narrowing to f32.
   let resid = (resid.sqrt() * (1.0 + 1e-6)) as f32;
   Some((scale, resid, code_sq.sqrt() as f32))
}

/// Writes the sketch row of `v`. `norm` is the norm the exact distance uses
/// (the stored cosine norm, or ‖v‖ for L2 and IP).
pub(crate) fn encode(v: &[f32], norm: f32, out: &mut [u32]) {
   let (head, body) = out.split_at_mut(HEADER);
   let codes = &mut bytemuck::cast_slice_mut::<u32, i8>(body)[..v.len()];
   match quantize(v, codes) {
      Some((scale, resid, code_norm)) => {
         head[0] = scale.to_bits();
         head[1] = norm.to_bits();
         head[2] = resid.to_bits();
         head[3] = code_norm.to_bits();
      }
      None => {
         // No bound: the row is always re-scored exactly.
         codes.fill(0);
         head[0] = 0f32.to_bits();
         head[1] = f32::NAN.to_bits();
         head[2] = f32::INFINITY.to_bits();
         head[3] = 0f32.to_bits();
      }
   }
}

/// A query prepared for sketch scoring.
pub(crate) struct SketchQuery {
   codes: Vec<i8>,
   scale: f32,
   /// The norm the exact distance divides by (cosine) or ‖q‖ (L2, IP).
   norm: f32,
   resid: f32,
}

impl SketchQuery {
   /// `None` when `q` is not finite; such queries take the plain scan.
   pub(crate) fn new(metric: DistanceMetric, q: &[f32], cosine_norm: f32) -> Option<Self> {
      let mut codes = vec![0i8; q.len()];
      let (scale, resid, _) = quantize(q, &mut codes)?;
      let norm = match metric {
         DistanceMetric::Cosine => cosine_norm,
         _ => kernels::norm_sq_f32(q).sqrt(),
      };
      norm.is_finite().then_some(Self {
         codes,
         scale,
         norm,
         resid,
      })
   }
}

/// Lower and upper bounds on the internal distance `HnswGraph::dist` returns
/// for the row's vector. Unbounded rows give (−∞, +∞).
#[inline(always)]
pub(crate) fn bounds<K: Kernel>(
   k: K,
   metric: DistanceMetric,
   q: &SketchQuery,
   row: &[u32],
   dims: usize,
) -> (f32, f32) {
   let scale = f32::from_bits(row[0]);
   let norm = f32::from_bits(row[1]);
   let resid = f32::from_bits(row[2]);
   let code_norm = f32::from_bits(row[3]);
   let codes = &bytemuck::cast_slice::<u32, i8>(&row[HEADER..])[..dims];
   let est = k.dot_i8(&q.codes, codes) as f32 * q.scale * scale;
   let err = q.norm * resid + q.resid * code_norm;
   let (lo, hi) = match metric {
      DistanceMetric::Cosine => {
         let denom = q.norm * norm;
         let slack = SLACK;
         (
            1.0 - (est + err) / denom - slack,
            1.0 - (est - err) / denom + slack,
         )
      }
      DistanceMetric::L2 => {
         let sq = q.norm * q.norm + norm * norm;
         let slack = SLACK * sq;
         (
            sq - 2.0 * (est + err) - slack,
            sq - 2.0 * (est - err) + slack,
         )
      }
      DistanceMetric::Ip => {
         let slack = SLACK * q.norm * norm;
         (-(est + err) - slack, -(est - err) + slack)
      }
   };
   // NaN anywhere (an unbounded row) fails this test.
   if lo <= hi {
      (lo, hi)
   } else {
      (f32::NEG_INFINITY, f32::INFINITY)
   }
}

/// Per-range result of a pruned scan: the `k` smallest upper bounds seen and
/// every row whose lower bound could still make the top-`k`.
#[derive(Default)]
pub(crate) struct Partial {
   pub(crate) upper: Vec<f32>,
   pub(crate) cands: Vec<(f32, u32)>,
}

/// Collects candidates for one query over one scan range.
pub(crate) struct Pruner {
   k: usize,
   upper: BinaryHeap<Dist>,
   cands: Vec<(f32, u32)>,
   /// Candidate count that triggers the next trim; doubles when a trim
   /// frees little, so loose bounds cannot make trimming quadratic.
   trim_at: usize,
}

impl Pruner {
   pub(crate) fn new(k: usize) -> Self {
      Self {
         k,
         upper: BinaryHeap::with_capacity(k + 1),
         cands: Vec::new(),
         trim_at: 64 * k.max(16),
      }
   }

   /// Current k-th smallest upper bound (+∞ until k rows were seen).
   #[inline(always)]
   fn threshold(&self) -> f32 {
      if self.upper.len() < self.k {
         f32::INFINITY
      } else {
         self.upper.peek().map_or(f32::INFINITY, |d| d.0)
      }
   }

   #[inline(always)]
   pub(crate) fn offer(&mut self, id: u32, (lo, hi): (f32, f32)) {
      let threshold = self.threshold();
      if lo > threshold {
         return;
      }
      self.cands.push((lo, id));
      if hi < threshold {
         if self.upper.len() == self.k {
            self.upper.pop();
         }
         self.upper.push(Dist(hi));
         // Early rows enter before the threshold settles; drop the ones it
         // has since ruled out so the list stays near its final size.
         if self.cands.len() >= self.trim_at {
            let t = self.threshold();
            self.cands.retain(|c| c.0 <= t);
            self.trim_at = self.trim_at.max(self.cands.len() * 2);
         }
      }
   }

   pub(crate) fn finish(self) -> Partial {
      Partial {
         upper: self.upper.into_iter().map(|d| d.0).collect(),
         cands: self.cands,
      }
   }
}

/// Ids that may belong to the global top-`k`: rows whose lower bound is at
/// most the k-th smallest upper bound over all parts.
pub(crate) fn survivors(parts: Vec<Partial>, k: usize) -> Vec<u32> {
   let mut upper: Vec<f32> = parts.iter().flat_map(|p| p.upper.iter().copied()).collect();
   let threshold = if upper.len() < k {
      f32::INFINITY
   } else {
      let (_, kth, _) = upper.select_nth_unstable_by(k - 1, f32::total_cmp);
      *kth
   };
   parts
      .into_iter()
      .flat_map(|p| p.cands)
      .filter(|c| c.0 <= threshold)
      .map(|c| c.1)
      .collect()
}
