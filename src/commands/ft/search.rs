//! FT.SEARCH execution: option parsing, filter scan (postings fast
//! path), KNN (graph/FLAT, filtered and not), and range queries.

use super::doc::{cond_matches, doc_fields, doc_scalar, doc_storage_matches, doc_vector, DocHit, SearchOpts};
use crate::search::query::{parse_query, Cond};
use crate::search::{IndexDef, QueryDist, Resolved};
use crate::storage::store::Store;
use crate::utils::resp;
use std::sync::Arc;

pub(super) fn parse_search_options(
    args: &[&str],
    binaries: &[(usize, Vec<u8>)],
) -> Result<SearchOpts, String> {
    let mut opts = SearchOpts::default();
    let mut i = 0;
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_str() {
            "NOCONTENT" => {
                opts.no_content = true;
                i += 1;
            }
            "WITHSCORES" => {
                opts.with_scores = true;
                i += 1;
            }
            "WITHPAYLOADS" => {
                opts.with_payloads = true;
                i += 1;
            }
            "LIMIT" => {
                opts.limit_offset = args
                    .get(i + 1)
                    .and_then(|v| v.parse().ok())
                    .ok_or("bad LIMIT offset")?;
                opts.limit_num = args
                    .get(i + 2)
                    .and_then(|v| v.parse().ok())
                    .ok_or("bad LIMIT count")?;
                i += 3;
            }
            "RETURN" => {
                let count = args
                    .get(i + 1)
                    .and_then(|v| v.parse::<usize>().ok())
                    .ok_or("bad RETURN count")?;
                i += 2;
                opts.return_all = false;
                for _ in 0..count {
                    match args.get(i) {
                        Some(f) if f.eq_ignore_ascii_case("AS") => {
                            i += 2;
                        }
                        Some(f) => {
                            opts.return_fields.push(f.to_string());
                            i += 1;
                        }
                        None => return Err("bad RETURN list".into()),
                    }
                }
            }
            "PARAMS" => {
                let count = args
                    .get(i + 1)
                    .and_then(|v| v.parse::<usize>().ok())
                    .ok_or("bad PARAMS count")?;
                i += 2;
                let n = count.min(args.len().saturating_sub(i));
                let mut j = i;
                while j + 1 < i + n {
                    let value = binaries
                        .iter()
                        .find(|(pos, _)| *pos == j + 4)
                        .map(|(_, bytes)| bytes.clone())
                        .unwrap_or_else(|| args[j + 1].as_bytes().to_vec());
                    opts.params.push((args[j].to_string(), value));
                    j += 2;
                }
                i += n;
            }
            "SORTBY" => {
                opts.sortby_field = args.get(i + 1).map(|s| s.to_string());
                let mut step = 2;
                if let Some(dir) = args.get(i + 2) {
                    if dir.eq_ignore_ascii_case("DESC") {
                        opts.sortby_desc = true;
                        step = 3;
                    } else if dir.eq_ignore_ascii_case("ASC") {
                        step = 3;
                    }
                }
                i += step;
            }
            "DIALECT" => i += 2,
            "INKEYS" => {
                let count = args
                    .get(i + 1)
                    .and_then(|v| v.parse::<usize>().ok())
                    .ok_or("bad INKEYS count")?;
                i += 2;
                for _ in 0..count {
                    if let Some(k) = args.get(i) {
                        opts.ids.push(k.to_string());
                    }
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }
    Ok(opts)
}pub(super) fn ft_search(parts: &[&str], store: &Arc<Store>, out: &mut Vec<u8>) {
    ft_search_impl(parts, &[], store, out)
}pub(super) fn ft_search_impl(
    parts: &[&str],
    binaries: &[(usize, Vec<u8>)],
    store: &Arc<Store>,
    out: &mut Vec<u8>,
) {
    if parts.len() < 3 {
        return resp::write_store_err(out, "ERR wrong number of arguments for 'ft.search'");
    }
    let index = parts[1];
    let query_str = parts[2];
    let Some(def) = store.search_state().def_arc(index) else {
        return resp::write_store_err(out, "ERR Unknown index name");
    };
    let Ok(opts) = parse_search_options(&parts[3..], binaries) else {
        return resp::write_store_err(out, "ERR bad search options");
    };
    let Ok(pq) = parse_query(&def, query_str) else {
        return resp::write_store_err(out, "ERR syntax error");
    };
    ft_search_execute(store, out, def, opts, pq);
}

/// Exact scans dominate vector KNN latency; below this keyspace size the
/// fixed thread-spawn cost outweighs the speedup, so scans stay inline.
const PARALLEL_SCAN_MIN_KEYS: usize = 20_000;

/// Threads for one filtered exact scan. Concurrent clients each run their
/// own scan on their own worker thread, so the budget divides the machine
/// across in-flight searches: a lone client gets the box, eight clients
/// get ~one thread each. (Unfiltered KNN skips the keyspace scan entirely
/// and streams the contiguous index via `brute_search`.)
fn scan_threads(store: &Arc<Store>, shards: usize) -> usize {
    let cpus = num_cpus::get();
    let busy = store.connected_clients().saturating_sub(1).min(cpus);
    (cpus / (busy + 1)).clamp(1, 8).min(shards).max(1)
}

fn parallel_scan<F>(store: &Arc<Store>, scan: F) -> (Vec<(f32, String)>, usize)
where
    F: Fn(usize, usize) -> (Vec<(f32, String)>, usize) + Sync + Send,
{
    let shards = store.data.shard_count();
    let keys = store.dbsize();
    if keys < PARALLEL_SCAN_MIN_KEYS || shards < 2 {
        let (heap, total) = scan(0, shards);
        return (heap, total);
    }

    let threads = scan_threads(store, shards);
    if threads <= 1 {
        return scan(0, shards);
    }
    let per = shards.div_ceil(threads);
    let mut heaps: Vec<(Vec<(f32, String)>, usize)> = Vec::with_capacity(threads);
    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(threads);
        for t in 0..threads {
            let from = t * per;
            let to = ((t + 1) * per).min(shards);
            if from >= to {
                continue;
            }
            let sf = &scan;
            handles.push(s.spawn(move || sf(from, to)));
        }
        for h in handles {
            if let Ok(r) = h.join() {
                heaps.push(r)
            }
        }
    });

    let mut total = 0;
    let mut merged: Vec<(f32, String)> = Vec::new();
    for (heap, n) in heaps {
        total += n;
        merged.extend(heap);
    }
    merged.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    (merged, total)
}

#[allow(clippy::too_many_arguments)]
fn ft_search_execute(
    store: &Arc<Store>,
    out: &mut Vec<u8>,
    def: Arc<IndexDef>,
    opts: SearchOpts,
    pq: crate::search::query::ParsedQuery,
) {
    if let Some(knn) = &pq.knn {
        let needle = knn.param.trim_start_matches('$');
        let Some((_, query_vec)) = opts
            .params
            .iter()
            .find(|(k, _)| k.trim_start_matches('$') == needle)
        else {
            return resp::write_store_err(out, "ERR query params: missing vector");
        };
        return knn_search(store, &def, knn, query_vec.as_slice(), &opts, pq.filter, out);
    }
    if let Some(range) = &pq.range {
        // Radius: a `$param` or a literal number.
        let radius = range.radius_param.parse::<f64>().ok().or_else(|| {
            opts.params
                .iter()
                .find(|(k, _)| k.trim_start_matches('$') == range.radius_param.trim_start_matches('$'))
                .and_then(|(_, v)| std::str::from_utf8(v).ok().and_then(|s| s.parse::<f64>().ok()))
        });
        let needle = range.vector_param.trim_start_matches('$');
        let query_vec = opts
            .params
            .iter()
            .find(|(k, _)| k.trim_start_matches('$') == needle)
            .map(|(_, v)| v.clone());
        let (Some(radius), Some(query_vec)) = (radius, query_vec) else {
            return resp::write_store_err(out, "ERR query params: missing range params");
        };
        return range_search(store, &def, range, radius, &query_vec, &opts, (out, pq.filter));
    }


    let filter = pq.filter.as_ref();
    let scan = |from: usize, to: usize| -> Vec<DocHit> {
        let mut hits: Vec<DocHit> = Vec::new();
        store.data.for_each_shard_range(from, to, |key, val| {
            if val.is_expired() || !def.matches_key(key) || !doc_storage_matches(val, &def) {
                return;
            }
            if !opts.ids.is_empty() && !opts.ids.iter().any(|id| id == key) {
                return;
            }
            if let Some(cond) = filter
                && !cond_matches(cond, val, &def)
            {
                return;
            }
            hits.push(DocHit {
                key: key.to_string(),
                score: 1.0,
            });
        });
        hits
    };


    // Inverted-index fast path: exact tag/numeric/text predicates (and
    // their negations) resolve to a postings intersection or complement
    // — no keyspace scan, no per-doc tag split, numeric parse, or text
    // word-split, and only the LIMIT page materializes keys. Tag/text
    // globs, INKEYS/SORTBY, TTL presence, and a stale generation fall
    // through to the scan above.
    if opts.ids.is_empty()
        && opts.sortby_field.is_none()
        && let Some(fi) = store.search_state().filter_index(store, &def)
    {
        let resolved = match &pq.filter {
            Some(c) => fi.resolve(&def, c),
            None => Some(Resolved::All),
        };
        if let Some(res) = resolved {
            let (total, page): (usize, Vec<(u32, f64)>) = match &res {
                Resolved::All => {
                    let n = fi.doc_count();
                    let s = opts.limit_offset.min(n);
                    let e = (s + opts.limit_num).min(n);
                    (n, (s..e).map(|i| (i as u32, 1.0)).collect())
                }
                Resolved::Ids(ids) => {
                    let s = opts.limit_offset.min(ids.len());
                    let e = (s + opts.limit_num).min(ids.len());
                    let ranked = pq.filter.as_ref().and_then(|c| fi.bm25(&def, c, ids));
                    match ranked {
                        Some(scores) => {
                            let mut order: Vec<(u32, f64)> =
                                ids.iter().zip(scores).map(|(&d, sc)| (d, sc as f64)).collect();
                            let cmp = |a: &(u32, f64), b: &(u32, f64)| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0));
                            if e < order.len() {
                                order.select_nth_unstable_by(e, cmp);
                                order.truncate(e);
                            }
                            order.sort_unstable_by(cmp);
                            (ids.len(), order[s.min(order.len())..].to_vec())
                        }
                        None => (ids.len(), ids[s..e].iter().map(|&d| (d, 1.0)).collect()),
                    }
                }
            };
            let hits: Vec<DocHit> = page
                .iter()
                .filter_map(|&(id, score)| fi.key(id).map(|key| DocHit { key: key.to_string(), score }))
                .collect();
            return emit_search_page(&hits, total, &opts, out, store, &def);
        }
    }
    let mut hits = parallel_filter_scan(store, scan);
    emit_search_results(&mut hits, &opts, out, store, &def);
}

fn emit_search_results(
    hits: &mut [DocHit],
    opts: &SearchOpts,
    out: &mut Vec<u8>,
    store: &Store,
    def: &IndexDef,
) {
    if let Some(sort_field) = &opts.sortby_field {
        let desc = opts.sortby_desc;
        hits.sort_by(|a, b| {
            let av = sort_f64(store, def, &a.key, sort_field);
            let bv = sort_f64(store, def, &b.key, sort_field);
            let ord = av.partial_cmp(&bv).unwrap_or(std::cmp::Ordering::Equal);
            if desc {
                ord.reverse()
            } else {
                ord
            }
        });
    }

    let total = hits.len();
    let start = opts.limit_offset.min(total);
    let end = (start.saturating_add(opts.limit_num)).min(total);
    emit_search_page(&hits[start..end], total, opts, out, store, def);
}

/// Write one already-selected page plus its exact match total. The
/// inverted-index path hands in the LIMIT slice directly.
fn emit_search_page(
    hits: &[DocHit],
    total: usize,
    opts: &SearchOpts,
    out: &mut Vec<u8>,
    store: &Store,
    def: &IndexDef,
) {
    let per_doc = 1
        + usize::from(opts.with_scores)
        + usize::from(!opts.no_content)
        + usize::from(opts.with_payloads);
    resp::write_array_header(out, 1 + hits.len() * per_doc);
    resp::write_integer(out, total as i64);
    for hit in hits {
        resp::write_bulk_bytes(out, hit.key.as_bytes());
        if opts.with_scores {
            resp::write_bulk(out, &crate::utils::util::format_float(hit.score));
        }
        if opts.with_payloads {
            resp::write_nil(out);
        }
        if !opts.no_content {
            let fields = store
                .data
                .get_ref(&hit.key)
                .map(|val| doc_fields(&val, def, opts))
                .unwrap_or_default();
            resp::write_array_header(out, fields.len() * 2);
            for (k, v) in &fields {
                resp::write_bulk_bytes(out, k.as_bytes());
                resp::write_bulk_bytes(out, v);
            }
        }
    }
}

fn sort_f64(store: &Store, def: &IndexDef, key: &str, field: &str) -> f64 {
    store
        .data
        .get_ref(key)
        .and_then(|val| doc_scalar(&val, def, field))
        .unwrap_or(f64::NEG_INFINITY)
}

fn parallel_filter_scan<F>(store: &Arc<Store>, scan: F) -> Vec<DocHit>
where
    F: Fn(usize, usize) -> Vec<DocHit> + Sync + Send,
{
    let shards = store.data.shard_count();
    let keys = store.dbsize();
    if keys < PARALLEL_SCAN_MIN_KEYS || shards < 2 {
        return scan(0, shards);
    }
    let threads = num_cpus::get().clamp(2, 8).min(shards);
    let per = shards.div_ceil(threads);
    let mut all: Vec<DocHit> = Vec::new();
    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(threads);
        for t in 0..threads {
            let from = t * per;
            let to = ((t + 1) * per).min(shards);
            if from >= to {
                continue;
            }
            let sf = &scan;
            handles.push(s.spawn(move || sf(from, to)));
        }
        for h in handles {
            if let Ok(mut hits) = h.join() {
                all.append(&mut hits);
            }
        }
    });
    all
}

fn knn_search(
    store: &Arc<Store>,
    def: &Arc<IndexDef>,
    knn: &crate::search::query::KnnSpec,
    query_vec_raw: &[u8],
    opts: &SearchOpts,
    filter: Option<Cond>,
    out: &mut Vec<u8>,
) {
    let Some(field_def) = def.field(&knn.field) else {
        return resp::write_store_err(out, "ERR unknown vector field");
    };
    let Some(vattrs) = &field_def.vector else {
        return resp::write_store_err(out, "ERR field is not a vector");
    };
    let Some(qv_owned) = vattrs.decode(query_vec_raw) else {
        return resp::write_store_err(out, "ERR query vector dim mismatch");
    };
    let qv: &[f32] = &qv_owned;
    // KNN over an indexed vector field: walk the contiguous vector array
    // (HNSW graph traversal, or FLAT exact SIMD scan). The old flat path
    // re-scanned the hash keyspace per query — a random pointer chase plus
    // per-doc String alloc that capped single-query throughput at ~10 ms
    // and blocked client scaling.
    let indexed = vattrs.algorithm == "HNSW" || vattrs.algorithm == "FLAT";
    if indexed && let Some(entry) = store.search_state().vector_index_entry(store, def, &knn.field)
    {
        // Filtered KNN: resolve the predicate to postings, map to vector
        // ids once, then search the index with an allow-set — redis-style
        // pre-filtered vector search. Small subsets brute-force the exact
        // top-k; large ones traverse the graph unfiltered and collect only
        // matches (hnswlib semantics). Unfilterable predicates (globs,
        // TTL/stale state) fall through to the keyspace scan.
        let ef = knn.ef_runtime.unwrap_or((knn.k * 4).max(100)).max(knn.k);
        let graph = &entry.graph;
        // Quantized graphs return candidates in estimated order: fetch a
        // deeper head and rerank it with the documents' exact vectors.
        let depth = graph.rerank_depth(knn.k, ef);
        let fetch = |id: usize| {
            entry
                .key_for_id(id)
                .and_then(|key| crate::search::graph::doc_vector(store, def, &knn.field, vattrs, &key))
        };
        let finish = |r: Vec<(f32, usize)>| -> Vec<(f32, usize)> {
            if graph.is_quantized() { graph.rerank(qv, r, knn.k, &fetch) } else { r }
        };
        // None → unresolvable (scan); Some(None) → unrestricted;
        // Some(Some(ids)) → allow-set of vector ids.
        let plan: Option<Option<Vec<usize>>> = match &filter {
            None => Some(None),
            Some(cond) => {
                match store
                    .search_state()
                    .filter_index(store, def)
                    .and_then(|fi| fi.resolve_unordered(def, cond).map(|r| (fi, r)))
                {
                    None => None,
                    Some((_, Resolved::All)) => Some(None),
                    Some((fi, Resolved::Ids(docs))) => {
                        let map = fi.vids_for(&knn.field, &entry);
                        let vids: Vec<usize> = docs
                            .iter()
                            .filter_map(|&d| map.get(d as usize).copied().filter(|&v| v != u32::MAX))
                            .map(|v| v as usize)
                            .collect();
                        Some(Some(vids))
                    }
                }
            }
        };
        if let Some(plan) = plan {
            let (results, total) = match &plan {
                None => {
                    let r = if entry.is_linked() {
                        finish(graph.search(qv, depth, ef.max(depth)))
                    } else {
                        graph.brute_search(qv, knn.k)
                    };
                    (r, entry.len())
                }
                Some(vids) if vids.is_empty() => (Vec::new(), 0),
                Some(vids) => {
                    // Cost-based strategy: estimate brute-force vs graph
                    // traversal cost from the subset size, graph size, and
                    // vector dimension to pick the cheapest path.
                    let n_graph = entry.len().max(1);
                    let selectivity = vids.len() as f64 / n_graph as f64;
                    let dims = graph.dims();
                    // Brute cost ∝ |subset| × dim (memory bandwidth bound).
                    let brute_cost = vids.len() as f64 * dims as f64;
                    // Graph cost ∝ ef × dim (compute bound, fewer nodes).
                    let graph_ef = (ef.max(depth) as f64 * (1.0 / selectivity.max(0.01)).sqrt()).min(vids.len() as f64);
                    let graph_cost = graph_ef * dims as f64 * 1.5;
                    let use_brute = !entry.is_linked()
                        || vids.len() <= 256
                        || brute_cost < graph_cost;
                    let r = if use_brute {
                        finish(graph.brute_search_ids(qv, depth, vids))
                    } else {
                        let set = fyro_vector::hnsw::IdSet::from_ids(vids);
                        let acorn = selectivity < 0.3;
                        let mut r = graph.search_in_set(qv, depth, ef.max(depth), &set, acorn);
                        let mut ef_try = ef.max(80);
                        while r.len() < knn.k.min(set.len()) && ef_try < set.len().min(8192) {
                            ef_try = (ef_try * 4).min(set.len());
                            r = graph.search_in_set(qv, depth, ef_try.max(depth), &set, acorn);
                        }
                        finish(r)
                    };
                    (r, vids.len())
                }
            };
            let mut hits: Vec<(f32, String)> = Vec::with_capacity(results.len());
            for (d, id) in results {
                if let Some(key) = entry.key_for_id(id) {
                    hits.push((d, key.to_string()));
                }
            }
            return emit_knn(store, def, &hits, total, knn, opts, out);
        }
    }

    let k = knn.k;
    let scan_dist = QueryDist::new(vattrs.metric, qv);
    let scan = |from: usize, to: usize| -> (Vec<(f32, String)>, usize) {
        let mut heap: Vec<(f32, String)> = Vec::new();
        let mut cutoff = f32::INFINITY;
        let mut total = 0usize;
        store.data.for_each_shard_range(from, to, |key, val| {
            if val.is_expired() || !def.matches_key(key) || !doc_storage_matches(val, def) {
                return;
            }
            if let Some(cond) = &filter
                && !cond_matches(cond, val, def)
            {
                return;
            }
            if let Some(vec_bytes) = doc_vector(val, def, &knn.field)
                && let Some(doc) = vattrs.decode(vec_bytes)
            {
                total += 1;
                let d = scan_dist.distance(qv, &doc);
                if heap.len() < k {
                    heap.push((d, key.to_string()));
                    if heap.len() == k {
                        heap.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                        cutoff = heap[k - 1].0;
                    }
                } else if d < cutoff {
                    heap[k - 1] = (d, key.to_string());
                    let mut i = k - 1;
                    while i > 0 && heap[i].0 < heap[i - 1].0 {
                        heap.swap(i, i - 1);
                        i -= 1;
                    }
                    cutoff = heap[k - 1].0;
                }
            }
        });
        (heap, total)
    };

    let (mut heap, total) = parallel_scan(store, scan);
    heap.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    heap.truncate(k);
    emit_knn(store, def, &heap, total, knn, opts, out);
}

fn emit_knn(
    store: &Store,
    def: &IndexDef,
    top: &[(f32, String)],
    total: usize,
    knn: &crate::search::query::KnnSpec,
    opts: &SearchOpts,
    out: &mut Vec<u8>,
) {
    let score_alias = knn.as_field.as_str();
    let mut field_opts = SearchOpts::default();
    if !opts.return_all {
        field_opts.return_fields = opts.return_fields.clone();
        field_opts.return_all = false;
    }

    let per_doc = 1
        + usize::from(opts.with_scores)
        + usize::from(!opts.no_content);
    resp::write_array_header(out, 1 + top.len() * per_doc);
    resp::write_integer(out, total as i64);
    for (d, key) in top {
        resp::write_bulk_bytes(out, key.as_bytes());
        if opts.with_scores {
            resp::write_bulk(out, &format!("{:.6}", d));
        }
        if !opts.no_content {
            let mut fields = store
                .data
                .get_ref(key)
                .map(|val| doc_fields(&val, def, &field_opts))
                .unwrap_or_default();
            let mut seen = false;
            for (k, _) in &fields {
                if k == score_alias {
                    seen = true;
                }
            }
            if !seen {
                fields.push((score_alias.to_string(), format!("{:.6}", d).into_bytes()));
            }
            resp::write_array_header(out, fields.len() * 2);
            for (k2, v2) in &fields {
                resp::write_bulk_bytes(out, k2.as_bytes());
                resp::write_bulk_bytes(out, v2);
            }
        }
    }
}
fn range_search(
    store: &Arc<Store>,
    def: &IndexDef,
    range: &crate::search::query::RangeSpec,
    radius: f64,
    query_vec_raw: &[u8],
    opts: &SearchOpts,
    reply: (&mut Vec<u8>, Option<Cond>),
) {
    let (out, filter) = reply;
    let Some(field_def) = def.field(&range.field) else {
        return resp::write_store_err(out, "ERR unknown vector field");
    };
    let Some(vattrs) = &field_def.vector else {
        return resp::write_store_err(out, "ERR field is not a vector");
    };
    let Some(qv) = vattrs.decode(query_vec_raw) else {
        return resp::write_store_err(out, "ERR query vector dim mismatch");
    };
    let radius = radius as f32;
    let mut heap: Vec<(f32, String)> = match indexed_range(store, def, range, vattrs, &qv, radius, filter.as_ref()) {
        Some(h) => h,
        None => scan_range(store, def, range, vattrs, &qv, radius, filter.as_ref()),
    };
    finish_range(&mut heap, def, range, opts, out, store);
}

/// VECTOR_RANGE through the vector index (graph or FLAT array) when one is
/// available and the filter resolves to postings; None → keyspace scan.
fn indexed_range(
    store: &Arc<Store>,
    def: &IndexDef,
    range: &crate::search::query::RangeSpec,
    vattrs: &crate::search::VectorAttrs,
    qv: &[f32],
    radius: f32,
    filter: Option<&Cond>,
) -> Option<Vec<(f32, String)>> {
    let ss = store.search_state();
    let def_arc = ss.def_arc(&def.name)?;
    // Applies pending writes first; None while a first build is running.
    let entry = ss.vector_index_entry(store, &def_arc, &range.field)?;
    let set = match filter {
        None => None,
        Some(cond) => {
            let fi = ss.filter_index(store, &def_arc)?;
            match fi.resolve(def, cond)? {
                Resolved::All => None,
                Resolved::Ids(docs) => {
                    let map = fi.vids_for(&range.field, &entry);
                    let vids: Vec<usize> = docs
                        .iter()
                        .filter_map(|&d| map.get(d as usize).copied().filter(|&v| v != u32::MAX))
                        .map(|v| v as usize)
                        .collect();
                    Some(fyro_vector::hnsw::IdSet::from_ids(&vids))
                }
            }
        }
    };
    if set.as_ref().is_some_and(|s| s.is_empty()) {
        return Some(Vec::new());
    }
    let graph = &entry.graph;
    let mut hits = graph.range(qv, radius, 64, set.as_ref(), entry.is_linked());
    if graph.is_quantized() {
        // Estimated distances: re-score exactly, then apply the radius.
        let n = hits.len();
        let fetch = |id: usize| entry.key_for_id(id).and_then(|k| crate::search::graph::doc_vector(store, def, &range.field, vattrs, &k));
        hits = graph.rerank(qv, hits, n, &fetch).into_iter().filter(|h| h.0 <= radius).collect();
    }
    Some(hits.into_iter().filter_map(|(d, id)| entry.key_for_id(id).map(|k| (d, k.to_string()))).collect())
}

fn scan_range(
    store: &Store,
    def: &IndexDef,
    range: &crate::search::query::RangeSpec,
    vattrs: &crate::search::VectorAttrs,
    qv: &[f32],
    radius: f32,
    filter: Option<&Cond>,
) -> Vec<(f32, String)> {
    let scan_dist = QueryDist::new(vattrs.metric, qv);
    let mut heap: Vec<(f32, String)> = Vec::new();
    store.data.for_each(|key, val| {
        if val.is_expired() || !def.matches_key(key) || !doc_storage_matches(val, def) {
            return;
        }
        if let Some(cond) = filter
            && !cond_matches(cond, val, def)
        {
            return;
        }
        if let Some(vec_bytes) = doc_vector(val, def, &range.field)
            && let Some(doc) = vattrs.decode(vec_bytes)
        {
            let d = scan_dist.distance(qv, &doc);
            if d <= radius {
                heap.push((d, key.to_string()));
            }
        }
    });
    heap
}

fn finish_range(
    heap: &mut [(f32, String)],
    def: &IndexDef,
    range: &crate::search::query::RangeSpec,
    opts: &SearchOpts,
    out: &mut Vec<u8>,
    store: &Store,
) {
    heap.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    let score_alias = range.as_field.as_str();
    let mut field_opts = SearchOpts::default();
    if !opts.return_all {
        field_opts.return_fields = opts.return_fields.clone();
        field_opts.return_all = false;
    }

    let total = heap.len();
    let start = opts.limit_offset.min(total);
    let end = (start + opts.limit_num).min(total);
    let top = &heap[start..end];

    let per_doc = 1
        + usize::from(opts.with_scores)
        + usize::from(!opts.no_content);
    resp::write_array_header(out, 1 + top.len() * per_doc);
    resp::write_integer(out, total as i64);
    for (d, key) in top {
        resp::write_bulk_bytes(out, key.as_bytes());
        if opts.with_scores {
            resp::write_bulk(out, &format!("{:.6}", d));
        }
        if !opts.no_content {
            let mut fields = store
                .data
                .get_ref(key)
                .map(|val| doc_fields(&val, def, &field_opts))
                .unwrap_or_default();
            let mut seen = false;
            for (k, _) in &fields {
                if k == score_alias {
                    seen = true;
                }
            }
            if !seen {
                fields.push((score_alias.to_string(), format!("{:.6}", d).into_bytes()));
            }
            resp::write_array_header(out, fields.len() * 2);
            for (k2, v2) in &fields {
                resp::write_bulk_bytes(out, k2.as_bytes());
                resp::write_bulk_bytes(out, v2);
            }
        }
    }
}