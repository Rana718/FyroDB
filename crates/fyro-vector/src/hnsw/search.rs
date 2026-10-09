//! Query side: greedy upper-level descent + level-0 beam search, filtered
//! traversal, and exact (FLAT) scans fanned out over the pool.

use super::*;

/// Exact scans currently running: a scan that finds the machine free fans
/// out across the pool; one that sees a peer stays serial (N concurrent
/// clients already fill the cores, more threads only fight for bandwidth).
static BRUTE_INFLIGHT: AtomicUsize = AtomicUsize::new(0);

/// Exact scans below this many vectors never fan out.
const PARALLEL_SCAN_MIN: usize = 4096;

/// Queries one batched exact pass serves at most.
const FLAT_BATCH_MAX: usize = 32;

pub(crate) struct FlatReq {
    q: Vec<f32>,
    k: usize,
    ready: AtomicBool,
    result: Mutex<Option<Vec<(f32, usize)>>>,
}

/// Rendezvous for batched exact scans on one graph.  Uses a lock-free
/// MPSC queue (SegQueue) for submissions so concurrent submitters never
/// contend on a single mutex.  One submitter wins the scanner role via
/// an AtomicBool CAS; others spin-yield on their per-request `ready` flag.
pub(crate) struct FlatBatch {
    queue: crossbeam_queue::SegQueue<Arc<FlatReq>>,
    scanning: AtomicBool,
}

impl Default for FlatBatch {
    fn default() -> Self {
        Self {
            queue: crossbeam_queue::SegQueue::new(),
            scanning: AtomicBool::new(false),
        }
    }
}

impl HnswGraph {
    /// Approximate top-`k` by graph traversal with beam width `ef`.
    pub fn search(&self, q: &[f32], k: usize, ef: usize) -> Vec<(f32, usize)> {
        self.search_with(q, k, ef.max(k), Collect::Query)
    }

    /// How many candidates a quantized search should hand to `rerank` for
    /// a final top-`k`: the estimate's error decides how deep the exact
    /// pass must look (RaBitQ's 1-bit estimate needs more than int8).
    pub fn rerank_depth(&self, k: usize, ef: usize) -> usize {
        match self.quant {
            Quant::None => k,
            Quant::Int8 => ef.max(k * 2),
            Quant::RaBitQ => (k * 16).max(ef).max(k),
        }
    }

    /// Exact re-scoring of approximate candidates: `fetch(id)` supplies the
    /// exact vector (None drops the candidate). Returns the best `k` with
    /// reported (metric-unit) distances.
    pub fn rerank(
        &self,
        q: &[f32],
        cands: Vec<(f32, usize)>,
        k: usize,
        fetch: &dyn Fn(usize) -> Option<Vec<f32>>,
    ) -> Vec<(f32, usize)> {
        let mut out: Vec<(f32, usize)> = cands
            .into_iter()
            .filter_map(|(_, id)| fetch(id).map(|v| (crate::distance::distance(self.metric, q, &v), id)))
            .collect();
        out.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
        out.truncate(k);
        out
    }

    /// Graph search restricted to `allow`: traversal stays unfiltered (edges
    /// through non-matching nodes remain navigable) and only matches are
    /// collected. `ef` is widened so a selective filter still fills `k`.
    pub fn search_filtered(
        &self,
        q: &[f32],
        k: usize,
        ef: usize,
        allow: &(dyn Fn(usize) -> bool + Sync),
    ) -> Vec<(f32, usize)> {
        self.search_with(q, k, ef.max(k).max(k * 8), Collect::Filtered(allow))
    }

    /// Filtered search against an id bitset. `acorn` selects ACORN-1
    /// traversal (Patel et al., SIGMOD'24): only members are expanded, each
    /// non-member neighbor contributing its own members (two-hop), so the
    /// beam walks the predicate's subgraph instead of wading through
    /// non-matching regions — the right regime for mid selectivity. Without
    /// it, traversal is unrestricted and only members are collected (best
    /// when most nodes match). Falls back to the unrestricted walk when the
    /// predicate subgraph is not reachable from the entry.
    pub fn search_in_set(&self, q: &[f32], k: usize, ef: usize, set: &IdSet, acorn: bool) -> Vec<(f32, usize)> {
        if k == 0 || set.is_empty() {
            return Vec::new();
        }
        if acorn {
            let norm = self.norm_of(q);
            let g = self.inner.read();
            if norm == 0.0 || g.dims != q.len() {
                return Vec::new();
            }
            let enc = self.encode_query(q);
            let query = Query { v: q, norm, enc: &enc };
            let mut out = Vec::with_capacity(ef);
            with_scratch(|sc| self.acorn(&g, sc, query, k, ef.max(k), set, &mut out));
            if out.len() >= k.min(set.len()) {
                return out.into_iter().map(|(d, id)| (self.report(d), id as usize)).collect();
            }
        }
        self.search_with(q, k, ef.max(k).max(k * 8), Collect::Set(set))
    }

    isa_dispatch!(fn acorn => acorn_gen(g: &Inner, sc: &mut Scratch, q: Query, k: usize, ef: usize, set: &IdSet, out: &mut Vec<(f32, u32)>) -> ());

    #[allow(clippy::too_many_arguments)]
    #[inline(always)]
    fn acorn_gen<K: Kernel>(
        &self,
        kern: K,
        g: &Inner,
        sc: &mut Scratch,
        q: Query,
        k: usize,
        ef: usize,
        set: &IdSet,
        out: &mut Vec<(f32, u32)>,
    ) {
        out.clear();
        let e = self.entry.load(Ordering::Acquire);
        if e == NO_ENTRY {
            return;
        }
        let (mut cur, top) = unpack(e);
        if cur as usize >= g.len {
            return;
        }
        let mut cur_d = self.dist(kern, g, q, cur);
        for level in (1..=top).rev() {
            (cur, cur_d) = self.greedy(kern, g, sc, q, cur, cur_d, level);
        }
        let collect = Collect::Set(set);
        let epoch = sc.begin(g.len);
        let mut cand = std::mem::take(&mut sc.cand);
        let mut res = std::mem::take(&mut sc.res);
        let mut links = std::mem::take(&mut sc.links);
        let mut hop = Vec::new();
        let mut frontier: Vec<u32> = Vec::with_capacity(self.m0 * 2);
        cand.clear();
        res.clear();
        sc.visit(cur, epoch);
        cand.push(Reverse((Dist(cur_d), cur)));
        if collect.accepts(g, cur) {
            res.push((Dist(cur_d), cur));
        }
        let cap = self.m0 * 2;
        while let Some(Reverse((Dist(d), node))) = cand.pop() {
            if res.len() >= ef && res.peek().is_some_and(|w| d > w.0.0) {
                break;
            }
            self.read_links(g, node, 0, &mut links);
            frontier.clear();
            for &nb in links.iter() {
                if frontier.len() >= cap {
                    break;
                }
                if set.contains(nb as usize) {
                    if !sc.visit(nb, epoch) {
                        frontier.push(nb);
                    }
                    continue;
                }
                // Two-hop: a non-member bridges to its member neighbors.
                if sc.visit(nb, epoch) {
                    continue;
                }
                self.read_links(g, nb, 0, &mut hop);
                for &nb2 in hop.iter() {
                    if frontier.len() >= cap {
                        break;
                    }
                    if set.contains(nb2 as usize) && !sc.visit(nb2, epoch) {
                        frontier.push(nb2);
                    }
                }
            }
            for &x in &frontier {
                g.prefetch(x);
            }
            for &x in &frontier {
                let nd = self.dist(kern, g, q, x);
                if res.len() < ef || res.peek().is_some_and(|w| nd < w.0.0) {
                    cand.push(Reverse((Dist(nd), x)));
                    if collect.accepts(g, x) {
                        res.push((Dist(nd), x));
                        if res.len() > ef {
                            res.pop();
                        }
                    }
                }
            }
        }
        out.extend(res.drain().map(|(Dist(d), id)| (d, id)));
        out.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
        out.truncate(k);
        sc.cand = cand;
        sc.res = res;
        sc.links = links;
    }

    /// Every node within `radius` (reported metric units) of `q`,
    /// ascending. Graph searches widen until the beam's farthest result
    /// falls outside the radius (so nothing closer was cut off); FLAT
    /// graphs and small allow-lists scan exactly. Approximate on graphs,
    /// as RediSearch's HNSW range queries are.
    pub fn range(&self, q: &[f32], radius: f32, ef: usize, set: Option<&IdSet>, linked: bool) -> Vec<(f32, usize)> {
        let live = self.len();
        let exact_only = !linked || set.is_some_and(|s| s.len() <= 4096);
        let within = |v: Vec<(f32, usize)>| -> Vec<(f32, usize)> { v.into_iter().filter(|r| r.0 <= radius).collect() };
        if exact_only {
            let all = match set {
                Some(s) => self.brute_search_ids(q, s.len(), &s.ids()),
                None => self.brute_search(q, live),
            };
            return within(all);
        }
        let mut k = 64usize.max(ef);
        loop {
            let r = match set {
                Some(s) => self.search_in_set(q, k, k, s, false),
                None => self.search(q, k, k),
            };
            let saturated = r.len() == k && r.last().is_some_and(|x| x.0 <= radius);
            if !saturated || k >= live {
                return within(r);
            }
            k = (k * 4).min(live);
        }
    }

    fn search_with(&self, q: &[f32], k: usize, ef: usize, collect: Collect) -> Vec<(f32, usize)> {
        if k == 0 {
            return Vec::new();
        }
        let norm = self.norm_of(q);
        if norm == 0.0 {
            return Vec::new();
        }
        let g = self.inner.read();
        if g.dims != q.len() {
            return Vec::new();
        }
        let enc = self.encode_query(q);
        let query = Query { v: q, norm, enc: &enc };
        let mut out = Vec::with_capacity(ef);
        with_scratch(|sc| self.knn(&g, sc, query, k, ef, collect, &mut out));
        out.into_iter().map(|(d, id)| (self.report(d), id as usize)).collect()
    }

    isa_dispatch!(fn knn => knn_gen(g: &Inner, sc: &mut Scratch, q: Query, k: usize, ef: usize, collect: Collect, out: &mut Vec<(f32, u32)>) -> ());

    #[allow(clippy::too_many_arguments)]
    #[inline(always)]
    fn knn_gen<K: Kernel>(
        &self,
        kern: K,
        g: &Inner,
        sc: &mut Scratch,
        q: Query,
        k: usize,
        ef: usize,
        collect: Collect,
        out: &mut Vec<(f32, u32)>,
    ) {
        out.clear();
        // Small graphs: traversal overhead dominates — scan directly.
        if g.len > 0 && g.len < 500 {
            let mut heap: BinaryHeap<(Dist, u32)> = BinaryHeap::with_capacity(k + 1);
            for id in 0..g.len as u32 {
                if g.flag(id) & F_DELETED != 0 || !collect.accepts(g, id) {
                    continue;
                }
                let d = Dist(self.dist(kern, g, q, id));
                if heap.len() < k {
                    heap.push((d, id));
                } else if heap.peek().is_some_and(|top| d < top.0) {
                    heap.pop();
                    heap.push((d, id));
                }
            }
            out.extend(heap.into_iter().map(|(Dist(d), id)| (d, id)));
            out.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
            return;
        }
        let e = self.entry.load(Ordering::Acquire);
        if e == NO_ENTRY {
            return;
        }
        let (mut cur, top) = unpack(e);
        if cur as usize >= g.len {
            return;
        }
        let mut cur_d = self.dist(kern, g, q, cur);
        for level in (1..=top).rev() {
            (cur, cur_d) = self.greedy(kern, g, sc, q, cur, cur_d, level);
        }
        self.search_layer(kern, g, sc, q, &[(cur_d, cur)], ef, 0, collect, out);
        out.truncate(k);
    }

    /// Exact top-`k` over every live vector (FLAT, or HNSW fallback).
    ///
    /// Concurrent calls on one graph are batched: while a scan runs, new
    /// queries queue up and the next scan serves all of them in one pass
    /// (each vector is loaded once for up to `FLAT_BATCH_MAX` queries), so
    /// throughput under concurrency is no longer one full memory sweep per
    /// query. A lone query scans immediately — there is no batching window.
    pub fn brute_search(&self, q: &[f32], k: usize) -> Vec<(f32, usize)> {
        if k == 0 {
            return Vec::new();
        }
        let req = Arc::new(FlatReq {
            q: q.to_vec(),
            k,
            ready: AtomicBool::new(false),
            result: Mutex::new(None),
        });
        self.flat.queue.push(Arc::clone(&req));
        loop {
            if req.ready.load(Ordering::Acquire) {
                return req.result.lock().take().unwrap_or_default();
            }
            // Try to become the scanner.
            if self.flat.scanning.compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                let mut batch = Vec::with_capacity(FLAT_BATCH_MAX);
                while let Some(r) = self.flat.queue.pop() {
                    batch.push(r);
                    if batch.len() >= FLAT_BATCH_MAX { break; }
                }
                if batch.is_empty() {
                    self.flat.scanning.store(false, Ordering::Release);
                    if req.ready.load(Ordering::Acquire) {
                        return req.result.lock().take().unwrap_or_default();
                    }
                    std::hint::spin_loop();
                    continue;
                }
                let results = {
                    let g = self.inner.read();
                    self.exact_batch(&g, &batch)
                };
                for (r, res) in batch.iter().zip(results) {
                    *r.result.lock() = Some(res);
                    r.ready.store(true, Ordering::Release);
                }
                self.flat.scanning.store(false, Ordering::Release);
            } else {
                // Another thread is scanning — help with pool work or spin.
                if !pool::global().try_run_ticket() {
                    for _ in 0..32 {
                        std::hint::spin_loop();
                        if req.ready.load(Ordering::Acquire) {
                            return req.result.lock().take().unwrap_or_default();
                        }
                    }
                    std::thread::yield_now();
                }
            }
        }
    }

    /// One exact pass for several queries, split across the pool.
    fn exact_batch(&self, g: &Inner, reqs: &[Arc<FlatReq>]) -> Vec<Vec<(f32, usize)>> {
        let encs: Vec<QEnc> = reqs.iter().map(|r| self.encode_query(&r.q)).collect();
        let queries: Vec<(Query, usize)> = reqs
            .iter()
            .zip(&encs)
            .map(|(r, e)| (Query { v: &r.q, norm: self.norm_of(&r.q), enc: e }, r.k))
            .collect();
        let valid: Vec<bool> = queries.iter().map(|(q, _)| q.norm != 0.0 && q.v.len() == g.dims).collect();
        let n = g.len;
        let pool = pool::global();
        let threads = if n >= PARALLEL_SCAN_MIN { pool.threads().min(n / 1024).max(1) } else { 1 };
        let per = n.div_ceil(threads.max(1));
        let parts: Vec<Mutex<Vec<Vec<(f32, u32)>>>> = (0..threads).map(|_| Mutex::new(Vec::new())).collect();
        pool.parallel(threads, |t| {
            let lo = (t * per).min(n);
            let hi = ((t + 1) * per).min(n);
            *parts[t].lock() = self.scan_batch(g, &queries, &valid, lo, hi);
        });
        let mut merged: Vec<Vec<(f32, u32)>> = vec![Vec::new(); reqs.len()];
        for p in parts {
            for (i, v) in p.into_inner().into_iter().enumerate() {
                merged[i].extend(v);
            }
        }
        merged
            .into_iter()
            .zip(reqs)
            .map(|(mut v, r)| {
                v.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
                v.truncate(r.k);
                v.into_iter().map(|(d, id)| (self.report(d), id as usize)).collect()
            })
            .collect()
    }

    isa_dispatch!(fn scan_batch => scan_batch_gen(g: &Inner, queries: &[(Query, usize)], valid: &[bool], lo: usize, hi: usize) -> Vec<Vec<(f32, u32)>>);

    #[allow(clippy::too_many_arguments)]
    #[inline(always)]
    fn scan_batch_gen<K: Kernel>(
        &self,
        kern: K,
        g: &Inner,
        queries: &[(Query, usize)],
        valid: &[bool],
        lo: usize,
        hi: usize,
    ) -> Vec<Vec<(f32, u32)>> {
        if self.quant == Quant::None && g.has_flat_int8() && (hi - lo) >= 2048 {
            return self.scan_batch_two_pass(kern, g, queries, valid, lo, hi);
        }
        let mut heaps: Vec<BinaryHeap<(Dist, u32)>> =
            queries.iter().map(|(_, k)| BinaryHeap::with_capacity(k + 1)).collect();
        for id in lo..hi {
            if id + 4 < hi {
                g.prefetch((id + 4) as u32);
            }
            let id = id as u32;
            if g.flag(id) & F_DELETED != 0 {
                continue;
            }
            // The vector stays in L1 while every query in the batch uses it.
            for (i, (q, k)) in queries.iter().enumerate() {
                if !valid[i] {
                    continue;
                }
                let d = Dist(self.dist(kern, g, *q, id));
                let heap = &mut heaps[i];
                if heap.len() < *k {
                    heap.push((d, id));
                } else if heap.peek().is_some_and(|top| d < top.0) {
                    heap.pop();
                    heap.push((d, id));
                }
            }
        }
        heaps
            .into_iter()
            .map(|h| h.into_vec().into_iter().map(|(Dist(d), id)| (d, id)).collect())
            .collect()
    }

    /// Exact top-`k` over an explicit id subset (postings-resolved filter).
    pub fn brute_search_ids(&self, q: &[f32], k: usize, ids: &[usize]) -> Vec<(f32, usize)> {
        let g = self.inner.read();
        self.exact(&g, q, k, Some(ids))
    }

    /// Shared exact scan over all slots (`ids = None`) or an id subset.
    /// Splits across the pool when the machine is otherwise free and the
    /// range is large.
    fn exact(&self, g: &Inner, q: &[f32], k: usize, ids: Option<&[usize]>) -> Vec<(f32, usize)> {
        let norm = self.norm_of(q);
        if k == 0 || norm == 0.0 || g.dims != q.len() {
            return Vec::new();
        }
        let enc = self.encode_query(q);
        let query = Query { v: q, norm, enc: &enc };
        let n = ids.map_or(g.len, |s| s.len());
        let pool = pool::global();
        let prev = BRUTE_INFLIGHT.fetch_add(1, Ordering::Relaxed);
        let threads = if prev == 0 && n >= PARALLEL_SCAN_MIN {
            pool.threads().min(n / 1024).max(1)
        } else {
            1
        };
        let mut out: Vec<(f32, u32)> = if threads <= 1 {
            self.scan(g, query, k, 0, n, ids)
        } else {
            let per = n.div_ceil(threads);
            let parts: Vec<Mutex<Vec<(f32, u32)>>> = (0..threads).map(|_| Mutex::new(Vec::new())).collect();
            pool.parallel(threads, |t| {
                let lo = (t * per).min(n);
                let hi = ((t + 1) * per).min(n);
                *parts[t].lock() = self.scan(g, query, k, lo, hi, ids);
            });
            parts.into_iter().flat_map(|p| p.into_inner()).collect()
        };
        BRUTE_INFLIGHT.fetch_sub(1, Ordering::Relaxed);
        out.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
        out.truncate(k);
        out.into_iter().map(|(d, id)| (self.report(d), id as usize)).collect()
    }

    isa_dispatch!(fn scan => scan_gen(g: &Inner, q: Query, k: usize, lo: usize, hi: usize, ids: Option<&[usize]>) -> Vec<(f32, u32)>);

    #[allow(clippy::too_many_arguments)]
    #[inline(always)]
    fn scan_gen<K: Kernel>(
        &self,
        kern: K,
        g: &Inner,
        q: Query,
        k: usize,
        lo: usize,
        hi: usize,
        ids: Option<&[usize]>,
    ) -> Vec<(f32, u32)> {
        let span = if ids.is_some() { ids.unwrap().len() } else { hi - lo };
        if self.quant == Quant::None && g.has_flat_int8() && span >= 256 {
            return match ids {
                None => self.scan_two_pass(kern, g, q, k, lo, hi),
                Some(id_slice) => self.scan_ids_two_pass(kern, g, q, k, &id_slice[lo..hi]),
            };
        }
        // Max-heap capped at k: the top is the current worst.
        let mut heap: BinaryHeap<(Dist, u32)> = BinaryHeap::with_capacity(k + 1);
        let push = |heap: &mut BinaryHeap<(Dist, u32)>, id: u32| {
            if g.flag(id) & F_DELETED != 0 {
                return;
            }
            let d = Dist(self.dist(kern, g, q, id));
            if heap.len() < k {
                heap.push((d, id));
            } else if heap.peek().is_some_and(|top| d < top.0) {
                heap.pop();
                heap.push((d, id));
            }
        };
        match ids {
            None => {
                for id in lo..hi {
                    if id + 4 < hi {
                        g.prefetch((id + 4) as u32);
                    }
                    push(&mut heap, id as u32);
                }
            }
            Some(ids) => {
                for (i, &id) in ids[lo..hi].iter().enumerate() {
                    if let Some(&next) = ids.get(lo + i + 4)
                        && next < g.len
                    {
                        g.prefetch(next as u32);
                    }
                    if id < g.len {
                        push(&mut heap, id as u32);
                    }
                }
            }
        }
        heap.into_vec().into_iter().map(|(Dist(d), id)| (d, id)).collect()
    }

    /// Two-pass scan: first scan compact Int8 codes for top candidates,
    /// then rerank against exact f32 vectors.
    #[inline(always)]
    fn scan_two_pass<K: Kernel>(
        &self,
        kern: K,
        g: &Inner,
        q: Query,
        k: usize,
        lo: usize,
        hi: usize,
    ) -> Vec<(f32, u32)> {
        let rerank_k = (k * 4).max(32).min(hi - lo);
        let q_maxabs = q.v.iter().fold(0f32, |m, x| m.max(x.abs()));
        let q_scale = if q_maxabs > 0.0 { q_maxabs / 127.0 } else { 1.0 };
        let q_codes: Vec<i8> = q.v.iter().map(|&x| (x / q_scale).round().clamp(-127.0, 127.0) as i8).collect();
        // Pass 1: Int8 pre-filter.
        let mut pre_heap: BinaryHeap<(Dist, u32)> = BinaryHeap::with_capacity(rerank_k + 1);
        for id in lo..hi {
            if g.flag(id as u32) & F_DELETED != 0 {
                continue;
            }
            let stored = g.flat_int8_of(id as u32);
            let dot = kern.dot_i8(&q_codes, stored);
            let approx = match self.metric {
                DistanceMetric::Cosine => -(dot as f32 * q_scale * g.flat_scale(id as u32)),
                DistanceMetric::Ip => -(dot as f32 * q_scale * g.flat_scale(id as u32)),
                DistanceMetric::L2 => -(dot as f32 * q_scale * g.flat_scale(id as u32)),
            };
            let d = Dist(approx);
            if pre_heap.len() < rerank_k {
                pre_heap.push((d, id as u32));
            } else if pre_heap.peek().is_some_and(|top| d < top.0) {
                pre_heap.pop();
                pre_heap.push((d, id as u32));
            }
        }
        // Pass 2: exact rerank.
        let mut heap: BinaryHeap<(Dist, u32)> = BinaryHeap::with_capacity(k + 1);
        for (_, id) in pre_heap.into_vec() {
            g.prefetch(id);
            let d = Dist(self.dist(kern, g, q, id));
            if heap.len() < k {
                heap.push((d, id));
            } else if heap.peek().is_some_and(|top| d < top.0) {
                heap.pop();
                heap.push((d, id));
            }
        }
        heap.into_vec().into_iter().map(|(Dist(d), id)| (d, id)).collect()
    }

    /// Two-pass batched scan: Int8 pre-filter for all queries in one arena sweep,
    /// then per-query exact rerank of the shortlisted candidates.
    #[allow(clippy::too_many_arguments)]
    #[inline(always)]
    fn scan_batch_two_pass<K: Kernel>(
        &self,
        kern: K,
        g: &Inner,
        queries: &[(Query, usize)],
        valid: &[bool],
        lo: usize,
        hi: usize,
    ) -> Vec<Vec<(f32, u32)>> {
        let nq = queries.len();
        let rerank_ks: Vec<usize> = queries.iter().map(|(_, k)| (k * 4).max(32).min(hi - lo)).collect();
        // Encode all query vectors to Int8 once.
        let q_info: Vec<(Vec<i8>, f32)> = queries
            .iter()
            .zip(valid.iter())
            .map(|((q, _), &v)| {
                if !v {
                    return (Vec::new(), 0.0);
                }
                let q_maxabs = q.v.iter().fold(0f32, |m, x| m.max(x.abs()));
                let q_scale = if q_maxabs > 0.0 { q_maxabs / 127.0 } else { 1.0 };
                let codes = q.v.iter().map(|&x| (x / q_scale).round().clamp(-127.0, 127.0) as i8).collect();
                (codes, q_scale)
            })
            .collect();
        // Pass 1: Int8 pre-filter — single pass over arena, all queries share each vector load.
        let mut pre_heaps: Vec<BinaryHeap<(Dist, u32)>> =
            rerank_ks.iter().map(|&rk| BinaryHeap::with_capacity(rk + 1)).collect();
        for id in lo..hi {
            if id + 4 < hi {
                g.prefetch((id + 4) as u32);
            }
            let id = id as u32;
            if g.flag(id) & F_DELETED != 0 {
                continue;
            }
            let stored = g.flat_int8_of(id);
            let x_scale = g.flat_scale(id);
            for i in 0..nq {
                if !valid[i] {
                    continue;
                }
                let (ref q_codes, q_scale) = q_info[i];
                let dot = kern.dot_i8(q_codes, stored);
                let approx = Dist(-(dot as f32 * q_scale * x_scale));
                let heap = &mut pre_heaps[i];
                let rk = rerank_ks[i];
                if heap.len() < rk {
                    heap.push((approx, id));
                } else if heap.peek().is_some_and(|top| approx < top.0) {
                    heap.pop();
                    heap.push((approx, id));
                }
            }
        }
        // Pass 2: exact rerank per query.
        pre_heaps
            .into_iter()
            .zip(queries.iter().zip(valid.iter()))
            .map(|(pre_heap, ((q, k), &v))| {
                if !v {
                    return Vec::new();
                }
                let mut heap: BinaryHeap<(Dist, u32)> = BinaryHeap::with_capacity(k + 1);
                for (_, id) in pre_heap.into_vec() {
                    g.prefetch(id);
                    let d = Dist(self.dist(kern, g, *q, id));
                    if heap.len() < *k {
                        heap.push((d, id));
                    } else if heap.peek().is_some_and(|top| d < top.0) {
                        heap.pop();
                        heap.push((d, id));
                    }
                }
                heap.into_vec().into_iter().map(|(Dist(d), id)| (d, id)).collect()
            })
            .collect()
    }

    /// Two-pass scan over an explicit id subset: Int8 pre-filter then exact rerank.
    #[inline(always)]
    fn scan_ids_two_pass<K: Kernel>(
        &self,
        kern: K,
        g: &Inner,
        q: Query,
        k: usize,
        ids: &[usize],
    ) -> Vec<(f32, u32)> {
        let rerank_k = (k * 4).max(32).min(ids.len());
        let q_maxabs = q.v.iter().fold(0f32, |m, x| m.max(x.abs()));
        let q_scale = if q_maxabs > 0.0 { q_maxabs / 127.0 } else { 1.0 };
        let q_codes: Vec<i8> = q.v.iter().map(|&x| (x / q_scale).round().clamp(-127.0, 127.0) as i8).collect();
        // Pass 1: Int8 pre-filter over the id subset.
        let mut pre_heap: BinaryHeap<(Dist, u32)> = BinaryHeap::with_capacity(rerank_k + 1);
        for (i, &id) in ids.iter().enumerate() {
            if id >= g.len || g.flag(id as u32) & F_DELETED != 0 {
                continue;
            }
            if let Some(&next) = ids.get(i + 4) {
                if next < g.len {
                    g.prefetch(next as u32);
                }
            }
            let stored = g.flat_int8_of(id as u32);
            let dot = kern.dot_i8(&q_codes, stored);
            let approx = Dist(-(dot as f32 * q_scale * g.flat_scale(id as u32)));
            if pre_heap.len() < rerank_k {
                pre_heap.push((approx, id as u32));
            } else if pre_heap.peek().is_some_and(|top| approx < top.0) {
                pre_heap.pop();
                pre_heap.push((approx, id as u32));
            }
        }
        // Pass 2: exact rerank.
        let mut heap: BinaryHeap<(Dist, u32)> = BinaryHeap::with_capacity(k + 1);
        for (_, id) in pre_heap.into_vec() {
            g.prefetch(id);
            let d = Dist(self.dist(kern, g, q, id));
            if heap.len() < k {
                heap.push((d, id));
            } else if heap.peek().is_some_and(|top| d < top.0) {
                heap.pop();
                heap.push((d, id));
            }
        }
        heap.into_vec().into_iter().map(|(Dist(d), id)| (d, id)).collect()
    }
}
