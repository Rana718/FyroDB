use crate::DistanceMetric;
use crate::kernels::{self, Isa, Kernel};
use crate::pool;
use parking_lot::{Mutex, RwLock};
use std::cell::{RefCell, UnsafeCell};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering};

mod quant;
mod search;
#[cfg(test)]
mod tests;

pub use quant::Quant;
use quant::QEnc;

/// RediSearch defaults.
pub const DEFAULT_M: usize = 16;
pub const DEFAULT_EF_CONSTRUCTION: usize = 200;

const MAX_LEVEL: usize = 16;
const SEED: u64 = 0x9E37_79B9_7F4A_7C15;
const NO_ENTRY: u64 = u64::MAX;
/// Not live: a fresh slot, a tombstone, or a compacted (free) slot.
const F_DELETED: u8 = 1;
/// Prepared (vector stored) but not yet linked.
const F_PENDING: u8 = 2;
/// Was live and still has edges pointing at it; cleared by compaction.
const F_TOMB: u8 = 4;
/// Tombstones above this share of node slots make `compact_if_needed` run.
const COMPACT_RATIO: f64 = 0.10;

#[repr(C, align(64))]
struct Line(UnsafeCell<[u32; 16]>);

// SAFETY: vector words are written only under the exclusive structural
// lock; link words are only ever accessed through atomics.
unsafe impl Sync for Line {}

impl Line {
    const fn zero() -> Self {
        Line(UnsafeCell::new([0; 16]))
    }
}

#[derive(PartialEq, Clone, Copy, Debug)]
struct Dist(f32);
impl Eq for Dist {}
impl PartialOrd for Dist {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Dist {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

// epoch-stamped visited table: mark = one store, clear = free. Zero hot-path allocs.
struct Scratch {
    stamps: Vec<u32>,
    epoch: u32,
    cand: BinaryHeap<Reverse<(Dist, u32)>>,
    res: BinaryHeap<(Dist, u32)>,
    links: Vec<u32>,
    layer: Vec<(f32, u32)>,
    sel: Vec<(f32, u32)>,
    over: Vec<(f32, u32)>,
    over_sel: Vec<(f32, u32)>,
    rejected: Vec<(f32, u32)>,
    q_int8: Vec<i8>,
    acorn_hop: Vec<u32>,
    acorn_frontier: Vec<u32>,
}

impl Scratch {
    const fn new() -> Self {
        Scratch {
            stamps: Vec::new(),
            epoch: 0,
            cand: BinaryHeap::new(),
            res: BinaryHeap::new(),
            links: Vec::new(),
            layer: Vec::new(),
            sel: Vec::new(),
            over: Vec::new(),
            over_sel: Vec::new(),
            rejected: Vec::new(),
            q_int8: Vec::new(),
            acorn_hop: Vec::new(),
            acorn_frontier: Vec::new(),
        }
    }

    #[inline]
    fn begin(&mut self, bound: usize) -> u32 {
        if self.stamps.len() < bound {
            self.stamps.resize(bound.max(self.stamps.len() * 2).max(1024), 0);
        }
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            self.stamps.fill(0);
            self.epoch = 1;
        }
        self.epoch
    }

    /// True when `id` was already visited in this run.
    #[inline(always)]
    fn visit(&mut self, id: u32, epoch: u32) -> bool {
        let s = &mut self.stamps[id as usize];
        if *s == epoch {
            return true;
        }
        *s = epoch;
        false
    }
}

thread_local! {
    static SCRATCH: RefCell<Scratch> = const { RefCell::new(Scratch::new()) };
}

fn with_scratch<R>(f: impl FnOnce(&mut Scratch) -> R) -> R {
    SCRATCH.with(|sc| match sc.try_borrow_mut() {
        Ok(mut s) => f(&mut s),
        // Re-entrant use (a pool job run by a thread that is itself inside
        // a traversal, while it helps drain the queue): use a fresh one.
        Err(_) => f(&mut Scratch::new()),
    })
}

pub(crate) struct Inner {
    dims: usize,
    quant: Quant,
    /// u32 words of payload (encoded vector + scalars) before the links.
    pay: usize,
    /// u32 words per level-0 block (multiple of 16: whole cache lines).
    stride: usize,
    blocks: Vec<Line>,
    /// Node slots allocated (ids `0..len`).
    len: usize,
    flags: Vec<AtomicU8>,
    levels: Vec<u8>,
    locks: Vec<Mutex<()>>,
    /// Levels 1..=level, each `[count, M × id]`; empty for level-0 nodes.
    upper: Vec<Box<[AtomicU32]>>,
    /// Compacted ids ready for reuse (no edge points at them).
    free: Vec<u32>,
    /// Quantized graphs: the exact vector of nodes still being linked (and
    /// of a whole bulk build), so links are chosen with exact distances;
    /// released once linked — only codes stay resident.
    raw: Vec<Option<Box<[f32]>>>,
    /// For `Quant::None` graphs with FLAT scans: packed Int8 codes and
    /// per-vector scales for a two-pass pre-filter scan.  Lazily populated
    /// by `prepare`/`prepare_batch` on unquantized graphs.
    flat_int8: Vec<i8>,
    flat_scales: Vec<f32>,
}

impl Inner {
    #[inline(always)]
    fn base(&self, id: u32) -> *mut u32 {
        debug_assert!((id as usize) < self.len);
        // SAFETY: ids < len; blocks hold at least len * stride words.
        unsafe { (self.blocks.as_ptr() as *mut u32).add(id as usize * self.stride) }
    }

    #[inline(always)]
    fn flat_int8_of(&self, id: u32) -> &[i8] {
        let off = id as usize * self.dims;
        &self.flat_int8[off..off + self.dims]
    }

    #[inline(always)]
    fn flat_scale(&self, id: u32) -> f32 {
        self.flat_scales[id as usize]
    }

    #[inline]
    fn has_flat_int8(&self) -> bool {
        !self.flat_int8.is_empty()
    }

    /// The f32 vector of `id` (unquantized graphs only).
    #[inline(always)]
    pub(crate) fn vector_of(&self, id: usize) -> &[f32] {
        debug_assert_eq!(self.quant, Quant::None);
        // SAFETY: payload words are immutable while a shared lock is held.
        unsafe { std::slice::from_raw_parts(self.base(id as u32) as *const f32, self.dims) }
    }

    /// The encoded payload of `id`.
    #[inline(always)]
    fn payload(&self, id: u32) -> &[u32] {
        // SAFETY: as `vector_of`.
        unsafe { std::slice::from_raw_parts(self.base(id), self.pay) }
    }

    #[inline(always)]
    fn norm(&self, id: u32) -> f32 {
        f32::from_bits(self.payload(id)[quant::norm_offset(self.quant, self.dims)])
    }

    #[inline(always)]
    fn raw_of(&self, id: u32) -> Option<&[f32]> {
        self.raw.get(id as usize).and_then(|r| r.as_deref())
    }

    #[inline(always)]
    fn links(&self, id: u32, level: usize, m: usize, m0: usize) -> (&AtomicU32, &[AtomicU32]) {
        if level == 0 {
            // SAFETY: link words are only accessed through atomics.
            unsafe {
                let p = self.base(id).add(self.pay);
                return (AtomicU32::from_ptr(p), std::slice::from_raw_parts(p.add(1) as *const AtomicU32, m0));
            }
        }
        let u = &self.upper[id as usize];
        let off = (level - 1) * (m + 1);
        (&u[off], &u[off + 1..off + 1 + m])
    }

    #[inline(always)]
    fn flag(&self, id: u32) -> u8 {
        self.flags[id as usize].load(Ordering::Acquire)
    }

    #[inline(always)]
    fn level(&self, id: u32) -> usize {
        self.levels[id as usize] as usize
    }

    /// Hint the vector lines of `id` into L1 before its distance is taken.
    #[inline(always)]
    fn prefetch(&self, id: u32) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
            let p = self.base(id) as *const i8;
            // Cap explicit hints; the streamer covers the tail of long vectors.
            let bytes = (self.pay * 4).min(1024);
            let mut off = 0;
            while off < bytes {
                _mm_prefetch(p.add(off), _MM_HINT_T0);
                off += 64;
            }
        }
    }

    /// Hint the level-0 link line of `id` (the next node to expand).
    #[inline(always)]
    fn prefetch_links(&self, id: u32) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
            _mm_prefetch(self.base(id).add(self.pay) as *const i8, _MM_HINT_T0);
        }
    }

    #[inline(always)]
    fn prefetch_flat_int8(&self, id: u32) {
        if self.flat_int8.is_empty() {
            return;
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
            let p = self.flat_int8.as_ptr().add(id as usize * self.dims);
            let bytes = self.dims.min(1024);
            let mut off = 0;
            while off < bytes {
                _mm_prefetch(p.add(off), _MM_HINT_T0);
                off += 64;
            }
        }
    }
}

/// Persisted link structure (see `HnswGraph::dump`).
pub struct GraphDump {
    pub nodes: Vec<DumpNode>,
    pub entry: Option<u32>,
    /// RaBitQ codebook the codes were encoded against (empty otherwise).
    pub centroids: Vec<Vec<f32>>,
}

pub struct DumpNode {
    pub id: u32,
    pub level: u8,
    /// `links[level]` = neighbor ids at that level.
    pub links: Vec<Vec<u32>>,
}

/// Query vector plus its norm (cosine) and, for quantized graphs, its
/// encoding — computed once per search.
#[derive(Clone, Copy)]
pub(crate) struct Query<'a> {
    v: &'a [f32],
    norm: f32,
    enc: &'a QEnc,
}

static NO_ENC: QEnc = QEnc::None;

/// Which traversed nodes may enter a layer's result set. Everything
/// reachable is traversed (deleted and filtered-out nodes keep the graph
/// connected).
#[derive(Clone, Copy)]
pub(crate) enum Collect<'a> {
    /// Construction: anything not deleted — in-flight inserts included, so
    /// concurrently inserted neighbors can link to each other.
    Build,
    /// Queries: linked, live nodes.
    Query,
    /// Queries restricted to ids the predicate accepts.
    Filtered(&'a (dyn Fn(usize) -> bool + Sync)),
    /// Queries restricted to members of an id bitset (one AND per test).
    Set(&'a IdSet),
}

/// Dense bitset over vector ids: the allow-list of a filtered query.
/// Membership is one shift-and-mask, instead of a binary search per
/// traversed neighbor.
pub struct IdSet {
    bits: Vec<u64>,
    len: usize,
}

impl IdSet {
    pub fn from_ids(ids: &[usize]) -> Self {
        let max = ids.iter().copied().max().map_or(0, |m| m + 1);
        let mut bits = vec![0u64; max.div_ceil(64)];
        let mut len = 0;
        for &id in ids {
            let w = &mut bits[id / 64];
            let b = 1u64 << (id % 64);
            if *w & b == 0 {
                *w |= b;
                len += 1;
            }
        }
        Self { bits, len }
    }

    #[inline(always)]
    pub fn contains(&self, id: usize) -> bool {
        self.bits.get(id / 64).is_some_and(|w| w >> (id % 64) & 1 == 1)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn ids(&self) -> Vec<usize> {
        let mut out = Vec::with_capacity(self.len);
        for (w, &word) in self.bits.iter().enumerate() {
            let mut x = word;
            while x != 0 {
                out.push(w * 64 + x.trailing_zeros() as usize);
                x &= x - 1;
            }
        }
        out
    }
}

impl Collect<'_> {
    #[inline(always)]
    fn accepts(&self, g: &Inner, id: u32) -> bool {
        let f = g.flag(id);
        match self {
            Collect::Build => f & F_DELETED == 0,
            Collect::Query => f == 0,
            Collect::Filtered(p) => f == 0 && p(id as usize),
            Collect::Set(s) => f == 0 && s.contains(id as usize),
        }
    }
}

pub struct HnswGraph {
    metric: DistanceMetric,
    quant: Quant,
    rot: Option<quant::Rotation>,
    /// RaBitQ centroids; replaced only while the graph is empty.
    book: RwLock<Arc<quant::Codebook>>,
    m: usize,
    m0: usize,
    ef_construction: usize,
    ml: f64,
    inner: RwLock<Inner>,
    /// `(id << 8) | level`, or `NO_ENTRY`.
    entry: AtomicU64,
    /// Held for the whole insert of a node that raises the top level, as
    /// hnswlib does, so two concurrent new tops can't miss each other.
    entry_lock: Mutex<()>,
    /// Serializes link phases against compaction (queries never take it).
    maint: Mutex<()>,
    count: AtomicUsize,
    pending: AtomicUsize,
    deleted: AtomicUsize,
    /// Effective ef during bulk build; ramped from 32 to `ef_construction`.
    build_ef: AtomicUsize,
    /// Cached level-0 entry hints: up to 16 spatially diverse nodes used as
    /// starting points for level-0 inserts, avoiding the greedy descent
    /// through upper HNSW layers for the 99.94% of nodes at level 0.
    entry_hints: Mutex<Vec<u32>>,
    flat: search::FlatBatch,
    last_shrink: AtomicUsize,
}

#[inline(always)]
fn pack(id: u32, level: usize) -> u64 {
    ((id as u64) << 8) | level as u64
}

#[inline(always)]
fn unpack(e: u64) -> (u32, usize) {
    ((e >> 8) as u32, (e & 0xFF) as usize)
}

#[inline]
fn splitmix(mut z: u64) -> u64 {
    z = z.wrapping_add(SEED);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

/// Define `$name(&self, args) -> ret` that runs the `#[inline(always)]`
/// generic `$generic::<K>(k, args)` inside a function compiled for the
/// detected ISA, so every kernel call inlines into the loop.
macro_rules! isa_dispatch {
    ($vis:vis fn $name:ident => $generic:ident ($($arg:ident : $ty:ty),*) -> $ret:ty) => {
        #[allow(clippy::too_many_arguments)]
        $vis fn $name(&self, $($arg: $ty),*) -> $ret {
            match kernels::isa() {
                #[cfg(target_arch = "x86_64")]
                Isa::Avx2(k) => {
                    #[target_feature(enable = "avx2,fma")]
                    #[allow(clippy::too_many_arguments)]
                    unsafe fn avx2(this: &HnswGraph, k: kernels::Avx2, $($arg: $ty),*) -> $ret {
                        this.$generic(k, $($arg),*)
                    }
                    // SAFETY: an `Avx2` kernel value proves CPU support.
                    unsafe { avx2(self, k, $($arg),*) }
                }
                #[cfg(target_arch = "x86_64")]
                Isa::Avx512(k) => {
                    #[target_feature(enable = "avx512f,avx512bw,avx512vnni,avx512vpopcntdq,avx2,fma")]
                    #[allow(clippy::too_many_arguments)]
                    unsafe fn avx512(this: &HnswGraph, k: kernels::Avx512, $($arg: $ty),*) -> $ret {
                        this.$generic(k, $($arg),*)
                    }
                    // SAFETY: an `Avx512` kernel value proves CPU support.
                    unsafe { avx512(self, k, $($arg),*) }
                }
                #[cfg(target_arch = "aarch64")]
                Isa::Neon(k) => self.$generic(k, $($arg),*),
                Isa::Scalar(k) => self.$generic(k, $($arg),*),
            }
        }
    };
}
pub(crate) use isa_dispatch;

impl HnswGraph {
    pub fn new(metric: DistanceMetric, dims: usize, m: usize, ef_construction: usize) -> Self {
        Self::with_quant(metric, dims, m, ef_construction, Quant::None)
    }

    /// A graph whose traversal payloads are `quant`-encoded (see `quant`).
    /// Search on a quantized graph returns candidates in estimated order;
    /// rerank the head with exact vectors (`rerank`).
    pub fn with_quant(metric: DistanceMetric, dims: usize, m: usize, ef_construction: usize, quant: Quant) -> Self {
        assert!(dims > 0);
        let m = m.clamp(2, 128);
        let m0 = m * 2;
        let pay = quant::payload_words(quant, dims);
        let stride = (pay + 1 + m0).div_ceil(16) * 16;
        Self {
            metric,
            quant,
            rot: (quant == Quant::RaBitQ).then(|| quant::Rotation::new(dims, dims as u64)),
            book: RwLock::new(Arc::new(quant::Codebook::zero(dims))),
            m,
            m0,
            ef_construction: ef_construction.clamp(1, 10_000),
            ml: 1.0 / (m as f64).ln(),
            inner: RwLock::new(Inner {
                dims,
                quant,
                pay,
                stride,
                blocks: Vec::new(),
                len: 0,
                flags: Vec::new(),
                levels: Vec::new(),
                locks: Vec::new(),
                upper: Vec::new(),
                free: Vec::new(),
                raw: Vec::new(),
                flat_int8: Vec::new(),
                flat_scales: Vec::new(),
            }),
            entry: AtomicU64::new(NO_ENTRY),
            entry_lock: Mutex::new(()),
            maint: Mutex::new(()),
            count: AtomicUsize::new(0),
            pending: AtomicUsize::new(0),
            deleted: AtomicUsize::new(0),
            build_ef: AtomicUsize::new(ef_construction.clamp(1, 10_000)),
            entry_hints: Mutex::new(Vec::new()),
            flat: search::FlatBatch::default(),
            last_shrink: AtomicUsize::new(0),
        }
    }

    pub fn metric(&self) -> DistanceMetric {
        self.metric
    }

    pub fn quant(&self) -> Quant {
        self.quant
    }

    /// Search distances are estimates; results need `rerank`.
    pub fn is_quantized(&self) -> bool {
        self.quant != Quant::None
    }

    pub fn m(&self) -> usize {
        self.m
    }

    pub fn ef_construction(&self) -> usize {
        self.ef_construction
    }

    pub fn dims(&self) -> usize {
        self.inner.read().dims
    }

    /// Live (non-deleted) nodes, linked or not.
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Allocation watermark: ids `0..node_count()` have storage.
    pub fn node_count(&self) -> usize {
        self.inner.read().len
    }

    /// Searchable (linked, live) node count.
    pub fn linked_len(&self) -> usize {
        self.len().saturating_sub(self.pending.load(Ordering::Acquire))
    }

    /// Tombstoned nodes not yet compacted away.
    pub fn deleted_len(&self) -> usize {
        self.deleted.load(Ordering::Acquire)
    }

    /// Approximate heap bytes held by the index.
    pub fn memory_bytes(&self) -> usize {
        let g = self.inner.read();
        g.blocks.capacity() * 64
            + g.flags.capacity() * 2
            + g.locks.capacity()
            + g.upper.capacity() * 16
            + g.upper.iter().map(|u| u.len() * 4).sum::<usize>()
            + g.free.capacity() * 4
            + g.raw.iter().flatten().map(|r| r.len() * 4).sum::<usize>()
    }

    /// True when `v` encodes to exactly `id`'s stored payload (for f32
    /// graphs: the same vector).
    pub fn vector_matches(&self, id: usize, v: &[f32]) -> bool {
        let g = self.inner.read();
        if id >= g.len || g.flag(id as u32) & F_DELETED != 0 || v.len() != g.dims {
            return false;
        }
        if self.quant == Quant::None {
            return g.vector_of(id) == v;
        }
        let mut tmp = vec![0u64; g.pay.div_ceil(2)];
        let buf = &mut bytemuck::cast_slice_mut::<u64, u32>(&mut tmp)[..g.pay];
        quant::encode(self.quant, self.rot.as_ref(), &self.book.read(), self.metric, v, buf);
        buf == g.payload(id as u32)
    }

    /// Copy of a live node's stored vector (unquantized graphs only).
    pub fn vector(&self, id: usize) -> Option<Vec<f32>> {
        let g = self.inner.read();
        (self.quant == Quant::None && id < g.len && g.flag(id as u32) & F_DELETED == 0)
            .then(|| g.vector_of(id).to_vec())
    }

    /// True when `id` is a live, linked (searchable) node.
    pub fn is_live(&self, id: usize) -> bool {
        let g = self.inner.read();
        id < g.len && g.flag(id as u32) == 0
    }

    #[inline(always)]
    fn norm_of(&self, v: &[f32]) -> f32 {
        match self.metric {
            DistanceMetric::Cosine => kernels::norm_sq_f32(v).sqrt().max(1e-10),
            _ => 1.0,
        }
    }

    /// Internal ranking distance: L2 is squared (sqrt only on reported
    /// results), cosine uses the precomputed norms, IP is negated.
    #[inline(always)]
    fn dist<K: Kernel>(&self, k: K, g: &Inner, q: Query, id: u32) -> f32 {
        if self.quant != Quant::None {
            return quant::distance(k, self.metric, q.enc, g.payload(id), g.dims);
        }
        let b = g.vector_of(id as usize);
        self.exact_f32(k, q.v, q.norm, b, g.norm(id))
    }

    #[inline(always)]
    fn exact_f32<K: Kernel>(&self, k: K, a: &[f32], na: f32, b: &[f32], nb: f32) -> f32 {
        match self.metric {
            DistanceMetric::L2 => k.l2sq(a, b),
            DistanceMetric::Cosine => 1.0 - k.dot(a, b) / (na * nb),
            DistanceMetric::Ip => -k.dot(a, b),
        }
    }

    /// Stored-vs-stored distance for neighbor selection: exact on f32
    /// graphs and whenever both raw vectors are still held (bulk builds);
    /// a code-vs-code estimate otherwise.
    #[inline(always)]
    fn dist_ids<K: Kernel>(&self, k: K, g: &Inner, a: u32, b: u32) -> f32 {
        if self.quant == Quant::None {
            let q = Query { v: g.vector_of(a as usize), norm: g.norm(a), enc: &NO_ENC };
            return self.dist(k, g, q, b);
        }
        if let (Some(ra), Some(rb)) = (g.raw_of(a), g.raw_of(b)) {
            let (na, nb) = match self.metric {
                DistanceMetric::Cosine => (k.dot(ra, ra).sqrt().max(1e-10), k.dot(rb, rb).sqrt().max(1e-10)),
                _ => (1.0, 1.0),
            };
            return self.exact_f32(k, ra, na, rb, nb);
        }
        quant::distance_codes(k, self.quant, self.metric, g.payload(a), g.payload(b), g.dims)
    }

    /// Encode a query for this graph's payloads.
    pub(crate) fn encode_query(&self, v: &[f32]) -> QEnc {
        quant::encode_query(self.quant, self.rot.as_ref(), &self.book.read(), self.metric, v)
    }

    // Only honored while graph is empty — existing codes are tied to the old codebook.
    pub fn train(&self, sample: &[Vec<f32>]) -> usize {
        if self.quant != Quant::RaBitQ || self.node_count() > 0 {
            return self.book.read().centroids.len();
        }
        let book = quant::Codebook::train(sample, self.inner.read().dims);
        let n = book.centroids.len();
        *self.book.write() = Arc::new(book);
        n
    }

    /// The centroids codes are encoded against (persistence).
    pub fn centroids(&self) -> Vec<Vec<f32>> {
        self.book.read().centroids.clone()
    }

    /// Install persisted centroids (empty graph only), as `train` would.
    pub fn set_centroids(&self, centroids: Vec<Vec<f32>>) {
        let dims = self.inner.read().dims;
        if self.quant == Quant::RaBitQ && self.node_count() == 0 && !centroids.is_empty() && centroids.iter().all(|c| c.len() == dims) {
            *self.book.write() = Arc::new(quant::Codebook { centroids });
        }
    }

    /// Internal ranking distance → reported metric value.
    #[inline(always)]
    fn report(&self, d: f32) -> f32 {
        match self.metric {
            DistanceMetric::L2 => d.max(0.0).sqrt(),
            _ => d,
        }
    }

    /// Geometric level with mL = 1/ln(M): P(level ≥ l) = M^-l. Seeded by
    /// id so rebuilds are deterministic.
    fn random_level(&self, id: usize) -> usize {
        let h = splitmix(id as u64);
        let u = ((h >> 11) as f64 + 0.5) / (1u64 << 53) as f64;
        ((-u.ln() * self.ml) as usize).min(MAX_LEVEL)
    }

    fn encode_flat_int8(g: &mut Inner, key_id: usize, vector: &[f32]) {
        let dims = g.dims;
        let needed = (key_id + 1) * dims;
        if g.flat_int8.len() < needed {
            g.flat_int8.resize(needed, 0);
        }
        if g.flat_scales.len() <= key_id {
            g.flat_scales.resize(key_id + 1, 0.0);
        }
        let maxabs = vector.iter().fold(0f32, |m, x| m.max(x.abs()));
        let scale = if maxabs > 0.0 { maxabs / 127.0 } else { 1.0 };
        g.flat_scales[key_id] = scale;
        let codes = &mut g.flat_int8[key_id * dims..(key_id + 1) * dims];
        for (c, &x) in codes.iter_mut().zip(vector) {
            *c = (x / scale).round().clamp(-127.0, 127.0) as i8;
        }
    }

    pub fn reserve(&self, n: usize) {
        let mut g = self.inner.write();
        let lines = n * g.stride / 16;
        if g.blocks.capacity() < lines {
            let extra = lines - g.blocks.len();
            g.blocks.reserve_exact(extra);
        }
        let extra = n.saturating_sub(g.flags.len());
        g.flags.reserve_exact(extra);
        g.levels.reserve_exact(extra);
        g.locks.reserve_exact(extra);
        g.upper.reserve_exact(extra);
    }

    fn grow(&self, g: &mut Inner, id: usize) {
        if id < g.len {
            return;
        }
        let n = id + 1;
        let lines = n * g.stride / 16;
        if g.blocks.len() < lines {
            // Within reserved capacity fill exactly; beyond it grow by half
            // so one-at-a-time inserts stay amortized O(1).
            let target = if lines <= g.blocks.capacity() {
                g.blocks.capacity()
            } else {
                lines.max(g.blocks.len() + g.blocks.len() / 2)
            };
            g.blocks.resize_with(target, Line::zero);
        }
        while g.flags.len() < n {
            g.flags.push(AtomicU8::new(F_DELETED));
            g.levels.push(0);
            g.locks.push(Mutex::new(()));
            g.upper.push(Box::new([]));
        }
        g.len = n;
    }

    pub fn prepare(&self, key_id: usize, vector: &[f32]) -> usize {
        self.prepare_at_level(key_id, vector, None)
    }

    fn prepare_at_level(&self, key_id: usize, vector: &[f32], level: Option<usize>) -> usize {
        assert!(key_id < (u32::MAX >> 1) as usize, "vector id space exhausted");
        let mut g = self.inner.write();
        assert_eq!(vector.len(), g.dims, "vector dimension mismatch");
        self.grow(&mut g, key_id);
        let id = key_id as u32;
        let prev = g.flag(id);
        let fresh = prev & F_DELETED != 0;
        let pay = g.pay;
        // SAFETY: exclusive lock; the block belongs to `id`.
        let payload = unsafe { std::slice::from_raw_parts_mut(g.base(id), pay) };
        quant::encode(self.quant, self.rot.as_ref(), &self.book.read(), self.metric, vector, payload);
        if self.quant == Quant::None {
            Self::encode_flat_int8(&mut g, key_id, vector);
        } else {
            if g.raw.len() <= key_id {
                g.raw.resize(key_id + 1, None);
            }
            g.raw[key_id] = Some(vector.into());
        }
        if !fresh {
            return g.level(id);
        }
        let lvl = level.unwrap_or_else(|| self.random_level(key_id));
        let (cnt, _) = g.links(id, 0, self.m, self.m0);
        cnt.store(0, Ordering::Relaxed);
        g.levels[key_id] = lvl as u8;
        g.upper[key_id] = (0..lvl * (self.m + 1)).map(|_| AtomicU32::new(0)).collect();
        if prev & F_TOMB != 0 {
            // An uncompacted tombstone coming back: its old in-edges now
            // lead to the new vector, which is harmless (they get re-pruned).
            self.deleted.fetch_sub(1, Ordering::AcqRel);
        } else if let Some(pos) = g.free.iter().position(|&f| f == id) {
            g.free.swap_remove(pos);
        }
        g.flags[key_id].store(F_PENDING, Ordering::Release);
        self.count.fetch_add(1, Ordering::AcqRel);
        self.pending.fetch_add(1, Ordering::AcqRel);
        lvl
    }

    /// Batch variant of `prepare`: takes the write lock once to allocate all
    /// slots, then encodes vectors in parallel on the thread pool.
    pub fn prepare_batch(&self, items: &[(usize, &[f32])]) {
        if items.is_empty() {
            return;
        }
        let max_id = items.iter().map(|&(id, _)| id).max().unwrap();
        let levels: Vec<usize> = items.iter().map(|&(id, _)| self.random_level(id)).collect();
        {
            let mut g = self.inner.write();
            assert!(items.iter().all(|&(_, v)| v.len() == g.dims), "dimension mismatch");
            self.grow(&mut g, max_id);
            for (i, &(key_id, _)) in items.iter().enumerate() {
                let id = key_id as u32;
                let prev = g.flag(id);
                let fresh = prev & F_DELETED != 0;
                if !fresh {
                    continue;
                }
                let lvl = levels[i];
                let (cnt, _) = g.links(id, 0, self.m, self.m0);
                cnt.store(0, Ordering::Relaxed);
                g.levels[key_id] = lvl as u8;
                g.upper[key_id] = (0..lvl * (self.m + 1)).map(|_| AtomicU32::new(0)).collect();
                if prev & F_TOMB != 0 {
                    self.deleted.fetch_sub(1, Ordering::AcqRel);
                } else if let Some(pos) = g.free.iter().position(|&f| f == id) {
                    g.free.swap_remove(pos);
                }
                g.flags[key_id].store(F_PENDING, Ordering::Release);
                self.count.fetch_add(1, Ordering::AcqRel);
                self.pending.fetch_add(1, Ordering::AcqRel);
            }
            if self.quant == Quant::None {
                let needed = (max_id + 1) * g.dims;
                if g.flat_int8.len() < needed {
                    g.flat_int8.resize(needed, 0);
                }
                if g.flat_scales.len() <= max_id {
                    g.flat_scales.resize(max_id + 1, 0.0);
                }
            } else {
                let max = max_id + 1;
                if g.raw.len() < max {
                    g.raw.resize(max, None);
                }
                for &(key_id, v) in items {
                    g.raw[key_id] = Some(v.into());
                }
            }
        }
        // Encode payloads in parallel under a read lock — each thread writes
        // to non-overlapping per-node memory regions.
        {
            let g = self.inner.read();
            let book = self.book.read();
            let rot = self.rot.as_ref();
            let quant = self.quant;
            let metric = self.metric;
            let pay = g.pay;
            let dims = g.dims;
            let next = AtomicUsize::new(0);
            pool::global().parallel(pool::global().threads().max(1), |_| {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= items.len() {
                        break;
                    }
                    let (key_id, v) = items[i];
                    // SAFETY: each thread writes to a distinct node's payload.
                    let payload = unsafe { std::slice::from_raw_parts_mut(g.base(key_id as u32), pay) };
                    quant::encode(quant, rot, &book, metric, v, payload);
                    if quant == Quant::None {
                        // SAFETY: each thread writes to a non-overlapping range.
                        let codes = unsafe {
                            std::slice::from_raw_parts_mut(
                                g.flat_int8.as_ptr() as *mut i8,
                                g.flat_int8.len(),
                            )
                        };
                        let scales = unsafe {
                            std::slice::from_raw_parts_mut(
                                g.flat_scales.as_ptr() as *mut f32,
                                g.flat_scales.len(),
                            )
                        };
                        let maxabs = v.iter().fold(0f32, |m, x| m.max(x.abs()));
                        let scale = if maxabs > 0.0 { maxabs / 127.0 } else { 1.0 };
                        scales[key_id] = scale;
                        let slot = &mut codes[key_id * dims..(key_id + 1) * dims];
                        for (c, &x) in slot.iter_mut().zip(v) {
                            *c = (x / scale).round().clamp(-127.0, 127.0) as i8;
                        }
                    }
                }
            });
        }
    }

    /// Link structure of every live node, for persistence. Tombstones are
    /// compacted away first: their vectors no longer exist in the keyspace
    /// to restore, and simply dropping edges to them would leave holes.
    pub fn dump(&self) -> GraphDump {
        if self.deleted_len() > 0 {
            self.compact();
        }
        let _m = self.maint.lock();
        let g = self.inner.read();
        let mut nodes = Vec::new();
        let mut links = Vec::new();
        for id in 0..g.len as u32 {
            if g.flag(id) != 0 {
                continue;
            }
            let lvl = g.level(id);
            let mut per_level = Vec::with_capacity(lvl + 1);
            for level in 0..=lvl {
                self.read_links(&g, id, level, &mut links);
                per_level.push(links.iter().copied().filter(|&e| g.flag(e) == 0).collect());
            }
            nodes.push(DumpNode { id, level: lvl as u8, links: per_level });
        }
        let entry = match self.entry.load(Ordering::Acquire) {
            NO_ENTRY => None,
            e => Some(unpack(e).0).filter(|&id| g.flag(id) == 0),
        };
        let centroids = if self.quant == Quant::RaBitQ { self.centroids() } else { Vec::new() };
        GraphDump { nodes, entry, centroids }
    }

    /// Recreate a dumped node: its vector, stored level and links, linked
    /// immediately (no search). Call for every node, then `restore_entry`.
    pub fn restore_node(&self, id: usize, vector: &[f32], level: usize, links: &[Vec<u32>]) {
        let level = level.min(MAX_LEVEL);
        {
            let mut g = self.inner.write();
            self.grow(&mut g, id);
        }
        self.prepare_at_level(id, vector, Some(level));
        let g = self.inner.read();
        let id32 = id as u32;
        for (lv, list) in links.iter().enumerate().take(level + 1) {
            let cap = if lv == 0 { self.m0 } else { self.m };
            let (cnt, slots) = g.links(id32, lv, self.m, self.m0);
            let n = list.len().min(cap);
            for (slot, &e) in slots.iter().zip(list.iter().take(n)) {
                slot.store(e, Ordering::Relaxed);
            }
            cnt.store(n as u32, Ordering::Release);
        }
        self.finish_link(&g, id32);
        drop(g);
        self.release_raw(&[id]);
    }

    /// Point the entry at `id` (its stored level) after a restore, or at
    /// the highest live node when `id` is unusable.
    pub fn restore_entry(&self, id: Option<usize>) {
        let g = self.inner.read();
        let pick = id
            .map(|i| i as u32)
            .filter(|&i| (i as usize) < g.len && g.flag(i) == 0)
            .or_else(|| (0..g.len as u32).filter(|&i| g.flag(i) == 0).max_by_key(|&i| g.levels[i as usize]));
        self.entry.store(pick.map_or(NO_ENTRY, |i| pack(i, g.level(i))), Ordering::Release);
    }

    // Reverse Cuthill-McKee reorder: cache-adjacent nodes, ~40% faster queries.
    // Tombstones dropped; returns new graph + old→new id map (u32::MAX = dropped).
    pub fn reordered(&self) -> (HnswGraph, Vec<u32>) {
        if self.deleted_len() > 0 {
            self.compact();
        }
        let _m = self.maint.lock();
        let g = self.inner.read();
        let n = g.len;
        let mut links = Vec::new();
        let degree = |id: u32, buf: &mut Vec<u32>| -> usize {
            self.read_links(&g, id, 0, buf);
            buf.len()
        };
        let mut order: Vec<u32> = Vec::with_capacity(n);
        let mut seen = vec![false; n];
        let mut queue = std::collections::VecDeque::new();
        let start = match self.entry.load(Ordering::Acquire) {
            NO_ENTRY => None,
            e => Some(unpack(e).0),
        };
        let mut next_root = 0u32;
        let mut nbrs: Vec<(usize, u32)> = Vec::new();
        let mut tmp = Vec::new();
        loop {
            let root = match start.filter(|&s| !seen[s as usize]) {
                Some(s) if order.is_empty() => s,
                _ => {
                    while (next_root as usize) < n && (seen[next_root as usize] || g.flag(next_root) & F_DELETED != 0) {
                        next_root += 1;
                    }
                    if next_root as usize >= n {
                        break;
                    }
                    next_root
                }
            };
            if g.flag(root) & F_DELETED != 0 {
                seen[root as usize] = true;
                continue;
            }
            seen[root as usize] = true;
            queue.push_back(root);
            while let Some(v) = queue.pop_front() {
                order.push(v);
                self.read_links(&g, v, 0, &mut links);
                nbrs.clear();
                for &u in &links {
                    if !seen[u as usize] && g.flag(u) & F_DELETED == 0 {
                        seen[u as usize] = true;
                        nbrs.push((degree(u, &mut tmp), u));
                    }
                }
                nbrs.sort_unstable();
                queue.extend(nbrs.iter().map(|&(_, u)| u));
            }
        }
        order.reverse();
        let mut map = vec![u32::MAX; n];
        for (new, &old) in order.iter().enumerate() {
            map[old as usize] = new as u32;
        }
        let out = HnswGraph::with_quant(self.metric, g.dims, self.m, self.ef_construction, self.quant);
        *out.book.write() = Arc::clone(&self.book.read());
        {
            let mut o = out.inner.write();
            let count = order.len();
            let lines = count * o.stride / 16;
            o.blocks.resize_with(lines, Line::zero);
            o.len = count;
            o.flags = (0..count).map(|_| AtomicU8::new(0)).collect();
            o.levels = vec![0; count];
            o.locks = (0..count).map(|_| Mutex::new(())).collect();
            o.upper = (0..count).map(|_| Box::new([]) as Box<[AtomicU32]>).collect();
            let mut remapped: Vec<u32> = Vec::new();
            for (new, &old) in order.iter().enumerate() {
                let new32 = new as u32;
                let lvl = g.level(old);
                // SAFETY: exclusive lock on `o`; blocks sized for `count`.
                unsafe {
                    std::ptr::copy_nonoverlapping(g.base(old), o.base(new32), g.pay);
                }
                o.levels[new] = lvl as u8;
                o.upper[new] = (0..lvl * (self.m + 1)).map(|_| AtomicU32::new(0)).collect();
                o.flags[new].store(g.flag(old) & !F_PENDING, Ordering::Relaxed);
                for level in 0..=lvl {
                    self.read_links(&g, old, level, &mut links);
                    remapped.clear();
                    remapped.extend(links.iter().map(|&e| map[e as usize]).filter(|&e| e != u32::MAX));
                    remapped.sort_unstable();
                    let (cnt, slots) = o.links(new32, level, self.m, self.m0);
                    for (slot, &e) in slots.iter().zip(&remapped) {
                        slot.store(e, Ordering::Relaxed);
                    }
                    cnt.store(remapped.len().min(slots.len()) as u32, Ordering::Release);
                }
            }
        }
        out.count.store(order.len(), Ordering::Release);
        if let Some(s) = start.map(|s| map[s as usize]).filter(|&s| s != u32::MAX) {
            let lvl = out.inner.read().level(s);
            out.entry.store(pack(s, lvl), Ordering::Release);
        } else {
            out.restore_entry(None);
        }
        (out, map)
    }

    /// Ids for `n` new nodes: compacted slots first, then fresh ones past
    /// the watermark. No existing edge references a returned id.
    pub fn alloc_ids(&self, n: usize) -> Vec<usize> {
        let g = self.inner.read();
        let mut out: Vec<usize> = g.free.iter().rev().take(n).map(|&i| i as usize).collect();
        let mut next = g.len;
        while out.len() < n {
            out.push(next);
            next += 1;
        }
        out
    }

    pub fn insert(&self, key_id: usize, vector: &[f32]) {
        self.prepare(key_id, vector);
        self.link_one(key_id);
    }

    pub fn link_one(&self, key_id: usize) {
        self.build_links(&[key_id], 1, &mut |_| {});
    }

    /// Soft-delete: the node stays traversable (its edges keep the graph
    /// connected) but is never returned; `compact` later unlinks it.
    pub fn mark_deleted(&self, key_id: usize) {
        let g = self.inner.read();
        if key_id >= g.len {
            return;
        }
        let f = &g.flags[key_id];
        if f.load(Ordering::Acquire) & F_DELETED != 0 {
            return;
        }
        let prev = f.fetch_or(F_DELETED | F_TOMB, Ordering::AcqRel);
        if prev & F_DELETED == 0 {
            self.count.fetch_sub(1, Ordering::AcqRel);
            self.deleted.fetch_add(1, Ordering::AcqRel);
            if prev & F_PENDING != 0 {
                f.fetch_and(!F_PENDING, Ordering::AcqRel);
                self.pending.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }

    /// Link a batch of prepared ids, every pool thread inserting in
    /// parallel. `progress(done)` fires after each chunk; all ids before
    /// `done` (in slice order) are linked at that point.
    pub fn build_links(&self, ids: &[usize], threads: usize, progress: &mut impl FnMut(usize)) {
        if ids.is_empty() {
            return;
        }
        let _m = self.maint.lock();
        let pool = pool::global();
        let threads = threads.clamp(1, pool.threads().max(1)).min(ids.len());
        let ef_max = self.ef_construction;
        // Only ramp for large bulk builds; small builds use full ef.
        let ramp = ids.len() >= 10_000;
        let ef_min = if ramp { (ef_max / 4).max(16) } else { ef_max };
        const RAMP_END: usize = 5_000;
        // Seed serially: parallel inserters need a connected core to search.
        let seed = ids.len().min(threads * 32);
        self.build_ef.store(ef_min, Ordering::Relaxed);
        {
            let g = self.inner.read();
            with_scratch(|sc| self.link_batch(&g, sc, &ids[..seed]));
            self.refresh_entry_hints(&g);
        }
        let mut done = seed;
        progress(done);
        let chunk = (threads * 512).max(4096);
        while done < ids.len() {
            if ramp {
                let ef = if done >= RAMP_END {
                    ef_max
                } else {
                    ef_min + (ef_max - ef_min) * done / RAMP_END
                };
                self.build_ef.store(ef, Ordering::Relaxed);
            }
            let part = &ids[done..(done + chunk).min(ids.len())];
            let next = AtomicUsize::new(0);
            let g = self.inner.read();
            pool.parallel(threads, |_| {
                with_scratch(|sc| loop {
                    let i = next.fetch_add(64, Ordering::Relaxed);
                    if i >= part.len() {
                        break;
                    }
                    self.link_batch(&g, sc, &part[i..(i + 64).min(part.len())]);
                })
            });
            self.refresh_entry_hints(&g);
            drop(g);
            done += part.len();
            progress(done);
        }
        self.build_ef.store(ef_max, Ordering::Relaxed);
        self.release_raw(ids);
        {
            let count = self.count.load(Ordering::Relaxed);
            let last = self.last_shrink.load(Ordering::Relaxed);
            let by_batch = ids.len() >= 1000;
            let by_milestone = count >= 1024 && count.leading_zeros() != last.leading_zeros();
            if by_batch || by_milestone {
                self.last_shrink.store(count, Ordering::Relaxed);
                let mut g = self.inner.write();
                g.blocks.shrink_to_fit();
                g.flags.shrink_to_fit();
                g.levels.shrink_to_fit();
                g.locks.shrink_to_fit();
                g.upper.shrink_to_fit();
                g.flat_int8.shrink_to_fit();
                g.flat_scales.shrink_to_fit();
            }
        }
    }

    /// Drop the exact vectors of linked `ids` (quantized graphs keep only
    /// codes resident).
    fn release_raw(&self, ids: &[usize]) {
        if self.quant == Quant::None {
            return;
        }
        let mut g = self.inner.write();
        for &id in ids {
            let linked = g.flags.get(id).is_some_and(|f| f.load(Ordering::Relaxed) & F_PENDING == 0);
            if linked && let Some(r) = g.raw.get_mut(id) {
                *r = None;
            }
        }
        if g.raw.iter().all(|r| r.is_none()) {
            g.raw = Vec::new();
        }
    }

    isa_dispatch!(fn link_batch => link_batch_gen(g: &Inner, sc: &mut Scratch, ids: &[usize]) -> ());

    #[inline(always)]
    fn link_batch_gen<K: Kernel>(&self, k: K, g: &Inner, sc: &mut Scratch, ids: &[usize]) {
        for &id in ids {
            self.link_node(k, g, sc, id as u32);
        }
    }

    /// hnswlib `addPoint`: greedy descent to the node's level, then per
    /// level an `ef_construction` beam search, heuristic selection, own
    /// links, and reverse links (re-selected by the heuristic on overflow).
    #[inline(always)]
    fn link_node<K: Kernel>(&self, k: K, g: &Inner, sc: &mut Scratch, id: u32) {
        if id as usize >= g.len || g.flag(id) != F_PENDING {
            return;
        }
        let lvl = g.level(id);
        let mut entry = self.entry.load(Ordering::Acquire);
        let mut _top = None;
        if entry == NO_ENTRY || lvl > unpack(entry).1 {
            _top = Some(self.entry_lock.lock());
            entry = self.entry.load(Ordering::Acquire);
            if entry == NO_ENTRY {
                self.finish_link(g, id);
                self.entry.store(pack(id, lvl), Ordering::Release);
                return;
            }
        }
        let (mut cur, top) = unpack(entry);
        let enc;
        let q = if self.quant == Quant::None {
            Query { v: g.vector_of(id as usize), norm: g.norm(id), enc: &NO_ENC }
        } else {
            let Some(raw) = g.raw_of(id) else {
                // No exact vector to link with (should not happen for a
                // prepared node): leave it unlinked rather than guess.
                return;
            };
            enc = self.encode_query(raw);
            Query { v: raw, norm: 1.0, enc: &enc }
        };
        let mut cur_d;
        if lvl == 0 && top > 0 {
            let hints = self.entry_hints.lock();
            if hints.len() >= 4 {
                cur_d = self.dist(k, g, q, cur);
                for &h in hints.iter() {
                    if h != id {
                        let d = self.dist(k, g, q, h);
                        if d < cur_d { cur = h; cur_d = d; }
                    }
                }
            } else {
                cur_d = self.dist(k, g, q, cur);
                for level in (1..=top).rev() {
                    (cur, cur_d) = self.greedy(k, g, sc, q, cur, cur_d, level);
                }
            }
        } else {
            cur_d = self.dist(k, g, q, cur);
            for level in (lvl + 1..=top).rev() {
                (cur, cur_d) = self.greedy(k, g, sc, q, cur, cur_d, level);
            }
        }
        let mut w = std::mem::take(&mut sc.layer);
        let mut sel = std::mem::take(&mut sc.sel);
        for level in (0..=lvl.min(top)).rev() {
            let ef = self.build_ef.load(Ordering::Relaxed);
            self.search_layer(k, g, sc, q, &[(cur_d, cur)], ef, level, Collect::Build, &mut w);
            w.retain(|&(_, n)| n != id);
            if let Some(&(d, n)) = w.first() {
                (cur_d, cur) = (d, n);
            }
            let cap = if level == 0 { self.m0 } else { self.m };
            self.select_heuristic(k, g, &mut sc.rejected, &w, self.m, &mut sel);
            {
                let _l = g.locks[id as usize].lock();
                let (cnt, slots) = g.links(id, level, self.m, self.m0);
                for (slot, &(_, n)) in slots.iter().zip(sel.iter()) {
                    slot.store(n, Ordering::Relaxed);
                }
                cnt.store(sel.len().min(cap) as u32, Ordering::Release);
            }
            for &(d, n) in sel.iter() {
                self.connect(k, g, sc, n, id, d, level, cap);
            }
        }
        sc.layer = w;
        sc.sel = sel;
        self.finish_link(g, id);
        if lvl > top {
            self.entry.store(pack(id, lvl), Ordering::Release);
        }
    }

    fn finish_link(&self, g: &Inner, id: u32) {
        let prev = g.flags[id as usize].fetch_and(!F_PENDING, Ordering::AcqRel);
        if prev & F_PENDING != 0 {
            self.pending.fetch_sub(1, Ordering::AcqRel);
        }
    }

    fn refresh_entry_hints(&self, g: &Inner) {
        const NUM_HINTS: usize = 16;
        let n = g.len;
        let mut hints = Vec::with_capacity(NUM_HINTS);
        if n == 0 {
            *self.entry_hints.lock() = hints;
            return;
        }
        let step = (n / NUM_HINTS).max(1);
        for i in 0..NUM_HINTS {
            let id = ((i * step).min(n - 1)) as u32;
            if g.flag(id) & (F_PENDING | F_DELETED) == 0 {
                hints.push(id);
            }
        }
        *self.entry_hints.lock() = hints;
    }

    /// Add `new` to `n`'s list at `level`; on overflow re-select the list
    /// with the heuristic over the existing edges plus `new`.
    #[allow(clippy::too_many_arguments)]
    #[inline(always)]
    fn connect<K: Kernel>(&self, k: K, g: &Inner, sc: &mut Scratch, n: u32, new: u32, d_new: f32, level: usize, cap: usize) {
        if g.flag(n) & F_DELETED != 0 || g.level(n) < level {
            return;
        }
        let _l = g.locks[n as usize].lock();
        let (cnt, slots) = g.links(n, level, self.m, self.m0);
        let len = (cnt.load(Ordering::Acquire) as usize).min(cap);
        if slots[..len].iter().any(|s| s.load(Ordering::Relaxed) == new) {
            return;
        }
        if len < cap {
            slots[len].store(new, Ordering::Relaxed);
            cnt.store(len as u32 + 1, Ordering::Release);
            return;
        }
        let mut over = std::mem::take(&mut sc.over);
        let mut over_sel = std::mem::take(&mut sc.over_sel);
        over.clear();
        over.push((d_new, new));
        for s in &slots[..len] {
            let e = s.load(Ordering::Relaxed);
            over.push((self.dist_ids(k, g, n, e), e));
        }
        over.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
        self.select_heuristic(k, g, &mut sc.rejected, &over, cap, &mut over_sel);
        for (slot, &(_, e)) in slots.iter().zip(over_sel.iter()) {
            slot.store(e, Ordering::Relaxed);
        }
        cnt.store(over_sel.len() as u32, Ordering::Release);
        sc.over = over;
        sc.over_sel = over_sel;
    }

    // Malkov Alg 4: keep only candidates closer to base than to any kept neighbor.
    // Rejects fill remaining slots so sparse regions maintain degree.
    #[inline(always)]
    fn select_heuristic<K: Kernel>(
        &self,
        k: K,
        g: &Inner,
        rejected: &mut Vec<(f32, u32)>,
        cands: &[(f32, u32)],
        m: usize,
        out: &mut Vec<(f32, u32)>,
    ) {
        out.clear();
        if cands.len() <= m {
            out.extend_from_slice(cands);
            return;
        }
        rejected.clear();
        for &(d, c) in cands {
            if out.len() >= m {
                break;
            }
            if out.iter().all(|&(_, r)| self.dist_ids(k, g, c, r) >= d) {
                out.push((d, c));
            } else {
                rejected.push((d, c));
            }
        }
        for &p in rejected.iter() {
            if out.len() >= m {
                break;
            }
            out.push(p);
        }
    }

    /// Greedy ef=1 walk on one level.
    #[allow(clippy::too_many_arguments)]
    #[inline(always)]
    fn greedy<K: Kernel>(&self, k: K, g: &Inner, sc: &mut Scratch, q: Query, mut cur: u32, mut cur_d: f32, level: usize) -> (u32, f32) {
        let mut links = std::mem::take(&mut sc.links);
        loop {
            let mut changed = false;
            self.read_links(g, cur, level, &mut links);
            for &nb in links.iter() {
                g.prefetch(nb);
            }
            for &nb in links.iter() {
                let d = self.dist(k, g, q, nb);
                if d < cur_d {
                    cur_d = d;
                    cur = nb;
                    changed = true;
                }
            }
            if !changed {
                sc.links = links;
                return (cur, cur_d);
            }
        }
    }

    #[inline(always)]
    fn read_links(&self, g: &Inner, id: u32, level: usize, out: &mut Vec<u32>) {
        out.clear();
        if g.level(id) < level {
            return;
        }
        let cap = if level == 0 { self.m0 } else { self.m };
        let (cnt, slots) = g.links(id, level, self.m, self.m0);
        let n = (cnt.load(Ordering::Acquire) as usize).min(cap);
        let len = g.len as u32;
        out.extend(slots[..n].iter().map(|s| s.load(Ordering::Relaxed)).filter(|&x| x < len));
    }

    /// Beam search on one level into `out` (ascending).
    #[allow(clippy::too_many_arguments)]
    #[inline(always)]
    fn search_layer<K: Kernel>(
        &self,
        k: K,
        g: &Inner,
        sc: &mut Scratch,
        q: Query,
        eps: &[(f32, u32)],
        ef: usize,
        level: usize,
        collect: Collect,
        out: &mut Vec<(f32, u32)>,
    ) {
        let epoch = sc.begin(g.len);
        let mut cand = std::mem::take(&mut sc.cand);
        let mut res = std::mem::take(&mut sc.res);
        let mut links = std::mem::take(&mut sc.links);
        cand.clear();
        res.clear();
        for &(d, ep) in eps {
            if sc.visit(ep, epoch) {
                continue;
            }
            cand.push(Reverse((Dist(d), ep)));
            if collect.accepts(g, ep) {
                res.push((Dist(d), ep));
            }
        }
        while let Some(Reverse((Dist(d), cur))) = cand.pop() {
            if res.len() >= ef && res.peek().is_some_and(|w| d > w.0.0) {
                break;
            }
            if let Some(Reverse((_, next))) = cand.peek() {
                g.prefetch_links(*next);
            }
            self.read_links(g, cur, level, &mut links);
            // Mark + prefetch the whole frontier first so the DRAM fetches
            // overlap, then compute distances.
            let mut np = 0;
            for i in 0..links.len() {
                let nb = links[i];
                if !sc.visit(nb, epoch) {
                    g.prefetch(nb);
                    links[np] = nb;
                    np += 1;
                }
            }
            for &nb in &links[..np] {
                let nd = self.dist(k, g, q, nb);
                if res.len() < ef || res.peek().is_some_and(|w| nd < w.0.0) {
                    cand.push(Reverse((Dist(nd), nb)));
                    if collect.accepts(g, nb) {
                        res.push((Dist(nd), nb));
                        if res.len() > ef {
                            res.pop();
                        }
                    }
                }
            }
        }
        out.clear();
        out.extend(res.drain().map(|(Dist(d), id)| (d, id)));
        out.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
        const SCRATCH_CAP_MAX: usize = 512;
        if cand.capacity() > SCRATCH_CAP_MAX * 2 {
            cand.shrink_to(SCRATCH_CAP_MAX);
        }
        if res.capacity() > SCRATCH_CAP_MAX * 2 {
            res.shrink_to(SCRATCH_CAP_MAX);
        }
        sc.cand = cand;
        sc.res = res;
        sc.links = links;
    }

    /// Compact when tombstones exceed `COMPACT_RATIO` of node slots.
    /// Returns the number of ids freed.
    pub fn compact_if_needed(&self) -> usize {
        let total = self.node_count();
        if total == 0 || (self.deleted_len() as f64) < COMPACT_RATIO * total as f64 {
            return 0;
        }
        self.compact()
    }

    // hnswlib repair: re-select edges around tombstones; freed ids join the free list.
    pub fn compact(&self) -> usize {
        let _m = self.maint.lock();
        let freed: Vec<u32> = {
            let g = self.inner.read();
            let n = g.len;
            let pool = pool::global();
            let next = AtomicUsize::new(0);
            pool.parallel(pool.threads().max(1), |_| {
                with_scratch(|sc| loop {
                    let i = next.fetch_add(256, Ordering::Relaxed);
                    if i >= n {
                        break;
                    }
                    self.repair_range(&g, sc, i..(i + 256).min(n));
                })
            });
            (0..n as u32).filter(|&id| g.flag(id) & F_TOMB != 0).collect()
        };
        let mut g = self.inner.write();
        for &id in &freed {
            let (cnt, _) = g.links(id, 0, self.m, self.m0);
            cnt.store(0, Ordering::Relaxed);
            g.flags[id as usize].store(F_DELETED, Ordering::Release);
            g.upper[id as usize] = Box::new([]);
            g.levels[id as usize] = 0;
            g.free.push(id);
        }
        let _ = self.deleted.try_update(Ordering::AcqRel, Ordering::Acquire, |d| Some(d.saturating_sub(freed.len())));
        // A freed entry point is replaced by the highest live node.
        let e = self.entry.load(Ordering::Acquire);
        if e != NO_ENTRY && g.flag(unpack(e).0) & F_DELETED != 0 {
            let best = (0..g.len as u32).filter(|&i| g.flag(i) == 0).max_by_key(|&i| g.levels[i as usize]);
            self.entry.store(best.map_or(NO_ENTRY, |i| pack(i, g.level(i))), Ordering::Release);
        }
        g.free.shrink_to_fit();
        drop(g);
        self.refresh_entry_hints(&self.inner.read());
        freed.len()
    }

    isa_dispatch!(fn repair_range => repair_range_gen(g: &Inner, sc: &mut Scratch, ids: std::ops::Range<usize>) -> ());

    #[inline(always)]
    fn repair_range_gen<K: Kernel>(&self, k: K, g: &Inner, sc: &mut Scratch, ids: std::ops::Range<usize>) {
        let mut own: Vec<u32> = Vec::new();
        let mut seen: Vec<u32> = Vec::new();
        for id in ids {
            let id = id as u32;
            if g.flag(id) != 0 {
                continue;
            }
            for level in 0..=g.level(id) {
                self.read_links(g, id, level, &mut own);
                if own.iter().all(|&e| g.flag(e) & F_DELETED == 0) {
                    continue;
                }
                let cap = if level == 0 { self.m0 } else { self.m };
                let mut cands = std::mem::take(&mut sc.over);
                cands.clear();
                seen.clear();
                seen.push(id);
                for &e in &own {
                    if g.flag(e) & F_DELETED == 0 {
                        if !seen.contains(&e) {
                            seen.push(e);
                            cands.push((self.dist_ids(k, g, id, e), e));
                        }
                        continue;
                    }
                    self.read_links(g, e, level, &mut sc.links);
                    for &x in sc.links.iter() {
                        if g.flag(x) & F_DELETED == 0 && !seen.contains(&x) {
                            seen.push(x);
                            cands.push((self.dist_ids(k, g, id, x), x));
                        }
                    }
                }
                cands.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
                let mut sel = std::mem::take(&mut sc.over_sel);
                self.select_heuristic(k, g, &mut sc.rejected, &cands, cap, &mut sel);
                {
                    let _l = g.locks[id as usize].lock();
                    let (cnt, slots) = g.links(id, level, self.m, self.m0);
                    for (slot, &(_, e)) in slots.iter().zip(sel.iter()) {
                        slot.store(e, Ordering::Relaxed);
                    }
                    cnt.store(sel.len() as u32, Ordering::Release);
                }
                sc.over_sel = sel;
                sc.over = cands;
            }
        }
    }
}
