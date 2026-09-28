//! FyroDB vector engine.
//!
//! - `kernels`: runtime-dispatched SIMD distance kernels
//! - `distance`: metric-level helpers over raw little-endian f32 bytes
//! - `hnsw`: the HNSW graph (also serves exact FLAT scans over its arena)

pub mod distance;
pub mod hnsw;
pub mod kernels;
pub mod pool;

pub use hnsw::Quant;
pub use kernels::convert::ElemType;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DistanceMetric {
    L2,
    Cosine,
    Ip,
}

impl DistanceMetric {
    pub fn as_str(self) -> &'static str {
        match self {
            DistanceMetric::L2 => "L2",
            DistanceMetric::Cosine => "COSINE",
            DistanceMetric::Ip => "IP",
        }
    }
}
