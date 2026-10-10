use crate::DistanceMetric;
use crate::kernels::Kernel;

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum Quant {
    #[default]
    None,
    Int8,
    RaBitQ,
}

impl Quant {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s.to_ascii_uppercase().as_str() {
            "NONE" | "FLOAT32" => Self::None,
            "INT8" | "SQ8" => Self::Int8,
            "RABITQ" => Self::RaBitQ,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Int8 => "INT8",
            Self::RaBitQ => "RABITQ",
        }
    }
}

/// Rotation / code width for RaBitQ: dims padded to a power of two ≥ 64.
pub(crate) fn rq_dim(dims: usize) -> usize {
    dims.next_power_of_two().max(64)
}

/// u32 words of per-node payload (vector encoding + scalars) before links.
pub(crate) fn payload_words(q: Quant, dims: usize) -> usize {
    match q {
        // vector, norm
        Quant::None => dims + 1,
        // scale, norm, codes
        Quant::Int8 => 2 + dims.div_ceil(4),
        // ‖r‖, factor, popcount, centroid, ‖x‖, pad, sign bits (u64-aligned
        // at +24 bytes)
        Quant::RaBitQ => 6 + 2 * (rq_dim(dims) / 64),
    }
}

/// Offset (u32 words) of the norm scalar in a payload.
pub(crate) fn norm_offset(q: Quant, dims: usize) -> usize {
    match q {
        Quant::None => dims,
        Quant::Int8 => 1,
        Quant::RaBitQ => 4,
    }
}

/// RaBitQ centroids (k-means over a build sample; one zero centroid until
/// trained). `c·c` cached for the L2/IP centroid terms.
pub(crate) struct Codebook {
    pub(crate) centroids: Vec<Vec<f32>>,
}

impl Codebook {
    pub(crate) fn zero(dims: usize) -> Self {
        Self { centroids: vec![vec![0.0; dims]] }
    }

    fn nearest(&self, v: &[f32]) -> usize {
        let mut best = (f32::INFINITY, 0usize);
        for (i, c) in self.centroids.iter().enumerate() {
            let d: f32 = c.iter().zip(v).map(|(a, b)| (a - b) * (a - b)).sum();
            if d < best.0 {
                best = (d, i);
            }
        }
        best.1
    }

    /// Deterministic k-means (k-means++ seeding, Lloyd iterations) over
    /// `sample`; k = min(16, sample/256), at least 1.
    pub(crate) fn train(sample: &[Vec<f32>], dims: usize) -> Self {
        let k = (sample.len() / 256).clamp(1, 16);
        if sample.is_empty() {
            return Self::zero(dims);
        }
        let mut rng = 0x2545_F491_4F6C_DD1Du64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let d2 = |a: &[f32], b: &[f32]| -> f32 { a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum() };
        let mut cents: Vec<Vec<f32>> = vec![sample[(next() as usize) % sample.len()].clone()];
        let mut dist: Vec<f32> = sample.iter().map(|v| d2(v, &cents[0])).collect();
        while cents.len() < k {
            let total: f64 = dist.iter().map(|&d| d as f64).sum();
            let mut pick = (next() as f64 / u64::MAX as f64) * total;
            let mut idx = 0;
            for (i, &d) in dist.iter().enumerate() {
                pick -= d as f64;
                if pick <= 0.0 {
                    idx = i;
                    break;
                }
            }
            cents.push(sample[idx].clone());
            let c = cents.last().unwrap().clone();
            for (d, v) in dist.iter_mut().zip(sample) {
                *d = d.min(d2(v, &c));
            }
        }
        let mut book = Self { centroids: cents };
        for _ in 0..8 {
            let mut sums = vec![vec![0f64; dims]; k];
            let mut counts = vec![0usize; k];
            for v in sample {
                let c = book.nearest(v);
                counts[c] += 1;
                for (s, &x) in sums[c].iter_mut().zip(v) {
                    *s += x as f64;
                }
            }
            for c in 0..k {
                if counts[c] > 0 {
                    book.centroids[c] = sums[c].iter().map(|s| (*s / counts[c] as f64) as f32).collect();
                }
            }
        }
        book
    }
}

/// In-place fast Walsh–Hadamard transform (unnormalized); len is a power of 2.
fn fwht(a: &mut [f32]) {
    let n = a.len();
    let mut h = 1;
    while h < n {
        for i in (0..n).step_by(h * 2) {
            for j in i..i + h {
                let (x, y) = (a[j], a[j + h]);
                a[j] = x + y;
                a[j + h] = x - y;
            }
        }
        h *= 2;
    }
}

// Randomized Hadamard: orthogonal, O(d log d), makes sign bits unbiased estimators.
pub(crate) struct Rotation {
    dp: usize,
    signs: [Vec<f32>; 3],
}

impl Rotation {
    pub(crate) fn new(dims: usize, seed: u64) -> Self {
        let dp = rq_dim(dims);
        let mut s = seed ^ 0xA076_1D64_78BD_642F;
        let mut sign = || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            if s & 1 == 0 { 1.0 } else { -1.0 }
        };
        let signs = [(0..dp).map(|_| sign()).collect(), (0..dp).map(|_| sign()).collect(), (0..dp).map(|_| sign()).collect()];
        Self { dp, signs }
    }

    pub(crate) fn dp(&self) -> usize {
        self.dp
    }

    /// `out = P · pad(x)`.
    pub(crate) fn apply(&self, x: &[f32], out: &mut Vec<f32>) {
        out.clear();
        out.extend_from_slice(x);
        out.resize(self.dp, 0.0);
        let norm = 1.0 / (self.dp as f32).sqrt();
        for s in &self.signs {
            for (o, &sg) in out.iter_mut().zip(s) {
                *o *= sg;
            }
            fwht(out);
            for o in out.iter_mut() {
                *o *= norm;
            }
        }
    }
}

/// Encode `v` into a payload slice of `payload_words(q, dims)` words.
pub(crate) fn encode(q: Quant, rot: Option<&Rotation>, book: &Codebook, metric: DistanceMetric, v: &[f32], out: &mut [u32]) {
    match q {
        Quant::None => {
            out[..v.len()].copy_from_slice(bytemuck::cast_slice(v));
            let norm = match metric {
                DistanceMetric::Cosine => crate::kernels::norm_sq_f32(v).sqrt().max(1e-10),
                _ => 1.0,
            };
            out[v.len()] = norm.to_bits();
        }
        Quant::Int8 => {
            let maxabs = v.iter().fold(0f32, |m, x| m.max(x.abs()));
            let scale = if maxabs > 0.0 { maxabs / 127.0 } else { 1.0 };
            let codes: &mut [i8] = &mut bytemuck::cast_slice_mut::<u32, i8>(&mut out[2..])[..v.len()];
            let mut sq = 0i64;
            for (c, &x) in codes.iter_mut().zip(v) {
                let q = (x / scale).round().clamp(-127.0, 127.0) as i8;
                *c = q;
                sq += q as i64 * q as i64;
            }
            out[0] = scale.to_bits();
            out[1] = (scale * (sq as f32).sqrt()).max(1e-10).to_bits();
        }
        Quant::RaBitQ => {
            let rot = rot.expect("RaBitQ graph has a rotation");
            let cid = book.nearest(v);
            let resid: Vec<f32> = v.iter().zip(&book.centroids[cid]).map(|(x, c)| x - c).collect();
            let norm = crate::kernels::norm_sq_f32(&resid).sqrt();
            let unit: Vec<f32> = if norm > 0.0 { resid.iter().map(|x| x / norm).collect() } else { vec![0.0; v.len()] };
            let mut r = Vec::with_capacity(rot.dp());
            rot.apply(&unit, &mut r);
            let words = rot.dp() / 64;
            let bits: &mut [u64] = &mut bytemuck::cast_slice_mut::<u32, u64>(&mut out[6..6 + 2 * words])[..words];
            bits.fill(0);
            let mut abs_sum = 0f32;
            for (d, &x) in r.iter().enumerate() {
                if x >= 0.0 {
                    bits[d / 64] |= 1 << (d % 64);
                }
                abs_sum += x.abs();
            }
            let pc: u32 = bits.iter().map(|w| w.count_ones()).sum();
            let factor = (abs_sum / (rot.dp() as f32).sqrt()).max(1e-6);
            out[0] = norm.to_bits();
            out[1] = factor.to_bits();
            out[2] = pc;
            out[3] = cid as u32;
            out[4] = crate::kernels::norm_sq_f32(v).sqrt().max(1e-10).to_bits();
            out[5] = 0;
        }
    }
}

/// One 4-bit bit-plane encoding of a rotated unit query.
pub(crate) struct RqPlanes {
    planes: Vec<u64>,
    lo: f32,
    delta: f32,
    sum: f32,
}

fn rq_planes(rot: &Rotation, v: &[f32]) -> (RqPlanes, f32) {
    let norm = crate::kernels::norm_sq_f32(v).sqrt();
    let unit: Vec<f32> = if norm > 0.0 { v.iter().map(|x| x / norm).collect() } else { vec![0.0; v.len()] };
    let mut r = Vec::with_capacity(rot.dp());
    rot.apply(&unit, &mut r);
    let lo = r.iter().copied().fold(f32::INFINITY, f32::min);
    let hi = r.iter().copied().fold(f32::NEG_INFINITY, f32::max);
    let delta = ((hi - lo) / 15.0).max(1e-12);
    let words = rot.dp() / 64;
    let mut planes = vec![0u64; 4 * words];
    let mut sum = 0f32;
    for (d, &x) in r.iter().enumerate() {
        let u = ((x - lo) / delta).round().clamp(0.0, 15.0) as u32;
        sum += lo + delta * u as f32;
        for j in 0..4 {
            if u >> j & 1 == 1 {
                planes[j * words + d / 64] |= 1 << (d % 64);
            }
        }
    }
    (RqPlanes { planes, lo, delta, sum }, norm)
}

/// Query-side encoding, computed once per search (or per inserted node).
pub(crate) enum QEnc {
    None,
    Int8 { codes: Vec<i8>, scale: f32, norm: f32 },
    /// One encoding of the residual `q − c` per centroid (every metric is
    /// scored through the centered residuals).
    Rq { parts: Vec<(RqPlanes, f32)>, qnorm: f32, inv_sqrt_dp: f32 },
}

pub(crate) fn encode_query(q: Quant, rot: Option<&Rotation>, book: &Codebook, metric: DistanceMetric, v: &[f32]) -> QEnc {
    match q {
        Quant::None => QEnc::None,
        Quant::Int8 => {
            let maxabs = v.iter().fold(0f32, |m, x| m.max(x.abs()));
            let scale = if maxabs > 0.0 { maxabs / 127.0 } else { 1.0 };
            let codes: Vec<i8> = v.iter().map(|&x| (x / scale).round().clamp(-127.0, 127.0) as i8).collect();
            let sq: i64 = codes.iter().map(|&c| c as i64 * c as i64).sum();
            QEnc::Int8 { codes, scale, norm: (scale * (sq as f32).sqrt()).max(1e-10) }
        }
        Quant::RaBitQ => {
            let rot = rot.expect("RaBitQ graph has a rotation");
            let qnorm = crate::kernels::norm_sq_f32(v).sqrt();
            // Score every metric through the centered residuals: the
            // uncentered direction estimate is far noisier (the reason the
            // codes are centered), and x·q follows from ‖x−q‖².
            let _ = metric;
            let parts = book
                .centroids
                .iter()
                .map(|c| {
                    let r: Vec<f32> = v.iter().zip(c).map(|(x, c)| x - c).collect();
                    rq_planes(rot, &r)
                })
                .collect();
            QEnc::Rq { parts, qnorm, inv_sqrt_dp: 1.0 / (rot.dp() as f32).sqrt() }
        }
    }
}

/// Estimated ranking distance between an encoded query and a payload.
#[inline(always)]
pub(crate) fn distance<K: Kernel>(k: K, metric: DistanceMetric, q: &QEnc, payload: &[u32], dims: usize) -> f32 {
    match q {
        QEnc::None => unreachable!("f32 payloads are scored by the f32 path"),
        QEnc::Int8 { codes, scale, norm } => {
            let x_scale = f32::from_bits(payload[0]);
            let x_norm = f32::from_bits(payload[1]);
            let x_codes = &bytemuck::cast_slice::<u32, i8>(&payload[2..])[..dims];
            let dot = k.dot_i8(codes, x_codes) as f32 * scale * x_scale;
            match metric {
                DistanceMetric::L2 => norm * norm + x_norm * x_norm - 2.0 * dot,
                DistanceMetric::Cosine => 1.0 - dot / (norm * x_norm),
                DistanceMetric::Ip => -dot,
            }
        }
        QEnc::Rq { parts, qnorm, inv_sqrt_dp } => {
            let r_norm = f32::from_bits(payload[0]);
            let factor = f32::from_bits(payload[1]);
            let pc = payload[2] as f32;
            let cid = payload[3] as usize;
            let (p, pnorm) = &parts[cid.min(parts.len() - 1)];
            let words = p.planes.len() / 4;
            let bits = &bytemuck::cast_slice::<u32, u64>(&payload[6..6 + 2 * words])[..words];
            let s = p.lo * pc + p.delta * k.plane_popcount(bits, &p.planes, 4) as f32;
            let est = ((2.0 * s - p.sum) * inv_sqrt_dp / factor).clamp(-1.0, 1.0);
            // ‖x−q‖² = ‖r_x‖² + ‖r_q‖² − 2‖r_x‖‖r_q‖·cos(r_x, r_q)
            let l2 = r_norm * r_norm + pnorm * pnorm - 2.0 * r_norm * pnorm * est;
            match metric {
                DistanceMetric::L2 => l2,
                DistanceMetric::Ip | DistanceMetric::Cosine => {
                    let x_norm = f32::from_bits(payload[4]);
                    let ip = 0.5 * (x_norm * x_norm + qnorm * qnorm - l2);
                    if metric == DistanceMetric::Ip { -ip } else { 1.0 - ip / (x_norm * qnorm.max(1e-10)) }
                }
            }
        }
    }
}

/// Code-vs-code estimate for neighbor pruning when neither raw vector is
/// available (incremental inserts after a build released them).
#[inline(always)]
pub(crate) fn distance_codes<K: Kernel>(k: K, quant: Quant, metric: DistanceMetric, a: &[u32], b: &[u32], dims: usize) -> f32 {
    match quant {
        Quant::None => unreachable!("f32 payloads are scored by the f32 path"),
        Quant::Int8 => {
            let (sa, na) = (f32::from_bits(a[0]), f32::from_bits(a[1]));
            let (sb, nb) = (f32::from_bits(b[0]), f32::from_bits(b[1]));
            let ca = &bytemuck::cast_slice::<u32, i8>(&a[2..])[..dims];
            let cb = &bytemuck::cast_slice::<u32, i8>(&b[2..])[..dims];
            let dot = k.dot_i8(ca, cb) as f32 * sa * sb;
            match metric {
                DistanceMetric::L2 => na * na + nb * nb - 2.0 * dot,
                DistanceMetric::Cosine => 1.0 - dot / (na * nb),
                DistanceMetric::Ip => -dot,
            }
        }
        Quant::RaBitQ => {
            let words = (a.len() - 6) / 2;
            let ba = &bytemuck::cast_slice::<u32, u64>(&a[6..6 + 2 * words])[..words];
            let bb = &bytemuck::cast_slice::<u32, u64>(&b[6..6 + 2 * words])[..words];
            let dp = (words * 64) as f32;
            let (ra, fa, ca, xa) = (f32::from_bits(a[0]), f32::from_bits(a[1]), a[3], f32::from_bits(a[4]));
            let (rb, fb, cb, xb) = (f32::from_bits(b[0]), f32::from_bits(b[1]), b[3], f32::from_bits(b[4]));
            // Residual directions are comparable only within one centroid;
            // across centroids treat them as orthogonal.
            let cos = if ca == cb {
                let ham: u32 = ba.iter().zip(bb).map(|(x, y)| (x ^ y).count_ones()).sum();
                ((1.0 - 2.0 * ham as f32 / dp) / (fa * fb)).clamp(-1.0, 1.0)
            } else {
                0.0
            };
            let l2 = ra * ra + rb * rb - 2.0 * ra * rb * cos;
            match metric {
                DistanceMetric::L2 => l2,
                DistanceMetric::Ip => -0.5 * (xa * xa + xb * xb - l2),
                DistanceMetric::Cosine => 1.0 - 0.5 * (xa * xa + xb * xb - l2) / (xa * xb),
            }
        }
    }
}
