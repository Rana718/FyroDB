use super::*;
use crate::distance::distance;

fn v(seed: f32, dims: usize) -> Vec<f32> {
   (0..dims)
      .map(|i| ((seed + i as f32) * 0.13).sin())
      .collect()
}

struct Lcg(u64);
impl Lcg {
   fn next(&mut self) -> f32 {
      self.0 = self
         .0
         .wrapping_mul(6364136223846793005)
         .wrapping_add(1442695040888963407);
      ((self.0 >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
   }
}

/// Gaussian-ish clusters: the shape where plain "keep the M closest"
/// neighbor selection disconnects clusters and the heuristic matters.
fn clustered(n: usize, dims: usize, clusters: usize, seed: u64) -> Vec<Vec<f32>> {
   let mut r = Lcg(seed);
   let cents: Vec<Vec<f32>> = (0..clusters)
      .map(|_| (0..dims).map(|_| r.next() * 4.0).collect())
      .collect();
   (0..n)
      .map(|i| {
         cents[i % clusters]
            .iter()
            .map(|c| c + 0.3 * (r.next() + r.next() + r.next()))
            .collect()
      })
      .collect()
}

fn exact_topk(
   metric: DistanceMetric,
   data: &[Vec<f32>],
   q: &[f32],
   k: usize,
   skip: impl Fn(usize) -> bool,
) -> Vec<usize> {
   let mut all: Vec<(f32, usize)> = data
      .iter()
      .enumerate()
      .filter(|(i, _)| !skip(*i))
      .map(|(i, v)| (distance(metric, q, v), i))
      .collect();
   all.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
   all.into_iter().take(k).map(|x| x.1).collect()
}

fn recall(
   g: &HnswGraph,
   metric: DistanceMetric,
   data: &[Vec<f32>],
   queries: &[Vec<f32>],
   ef: usize,
) -> f64 {
   let mut hits = 0;
   for q in queries {
      let want = exact_topk(metric, data, q, 10, |_| false);
      let got: Vec<usize> = g.search(q, 10, ef).iter().map(|x| x.1).collect();
      hits += got.iter().filter(|i| want.contains(i)).count();
   }
   hits as f64 / (queries.len() * 10) as f64
}

fn build(metric: DistanceMetric, data: &[Vec<f32>], m: usize, efc: usize) -> HnswGraph {
   let g = HnswGraph::new(metric, data[0].len(), m, efc);
   for (i, x) in data.iter().enumerate() {
      g.prepare(i, x);
   }
   let ids: Vec<usize> = (0..data.len()).collect();
   g.build_links(&ids, 8, &mut |_| {});
   g
}

#[test]
fn hnsw_recall_on_small_set() {
   let dims = 32;
   let g = HnswGraph::new(DistanceMetric::L2, dims, DEFAULT_M, DEFAULT_EF_CONSTRUCTION);
   let data: Vec<Vec<f32>> = (0..500).map(|i| v(i as f32, dims)).collect();
   for (i, x) in data.iter().enumerate() {
      g.insert(i, x);
   }
   let q = v(42.0, dims);
   let want = exact_topk(DistanceMetric::L2, &data, &q, 10, |_| false);
   let got: Vec<usize> = g.search(&q, 10, 100).iter().map(|x| x.1).collect();
   let hits = got.iter().filter(|i| want.contains(i)).count();
   assert!(hits >= 9, "recall@10 = {hits}/10");
}

#[test]
fn reported_distances_match_exact_kernel() {
   for metric in [
      DistanceMetric::Cosine,
      DistanceMetric::L2,
      DistanceMetric::Ip,
   ] {
      let dims = 16;
      let data: Vec<Vec<f32>> = (0..300).map(|i| v(i as f32 * 0.7, dims)).collect();
      let g = build(metric, &data, DEFAULT_M, DEFAULT_EF_CONSTRUCTION);
      let q = v(12.3, dims);
      let approx = g.search(&q, 10, 100);
      assert_eq!(approx.len(), 10);
      for (d, id) in &approx {
         let e = distance(metric, &q, &data[*id]);
         assert!(
            (d - e).abs() < 1e-3,
            "{metric:?}: graph dist {d} vs exact {e}"
         );
      }
      let brute = g.brute_search(&q, 10);
      for (d, id) in &brute {
         let e = distance(metric, &q, &data[*id]);
         assert!(
            (d - e).abs() < 1e-3,
            "{metric:?}: brute dist {d} vs exact {e}"
         );
      }
   }
}

#[test]
fn tombstones_are_skipped() {
   let dims = 8;
   let g = HnswGraph::new(DistanceMetric::L2, dims, DEFAULT_M, DEFAULT_EF_CONSTRUCTION);
   for i in 0..100 {
      g.insert(i, &v(i as f32, dims));
   }
   for i in 41..=50 {
      g.mark_deleted(i);
   }
   assert_eq!(g.len(), 90);
   let res = g.search(&v(42.0, dims), 10, 100);
   assert_eq!(res.len(), 10);
   for (_, id) in &res {
      assert!(!(41..=50).contains(id), "tombstone {id} returned");
   }
}

#[test]
fn brute_search_matches_exact_and_skips_tombstones() {
   let dims = 24;
   let n = 8192;
   let g = HnswGraph::new(
      DistanceMetric::Cosine,
      dims,
      DEFAULT_M,
      DEFAULT_EF_CONSTRUCTION,
   );
   let data: Vec<Vec<f32>> = (0..n).map(|i| v(i as f32 * 0.37, dims)).collect();
   for (i, x) in data.iter().enumerate() {
      g.prepare(i, x);
   }
   let q = v(500.0 * 0.37, dims);
   for i in (0..n).step_by(7) {
      g.mark_deleted(i);
   }
   let want = exact_topk(DistanceMetric::Cosine, &data, &q, 10, |i| i % 7 == 0);
   // Large enough to take the parallel path when the machine is idle.
   let res = g.brute_search(&q, 10);
   assert_eq!(res.len(), 10);
   for w in res.windows(2) {
      assert!(w[0].0 <= w[1].0 + 1e-6);
   }
   let got: Vec<usize> = res.iter().map(|x| x.1).collect();
   assert_eq!(got, want);
   let subset: Vec<usize> = (0..n).filter(|i| i % 3 == 0).collect();
   let want_sub = exact_topk(DistanceMetric::Cosine, &data, &q, 10, |i| {
      i % 3 != 0 || i % 7 == 0
   });
   let got_sub: Vec<usize> = g
      .brute_search_ids(&q, 10, &subset)
      .iter()
      .map(|x| x.1)
      .collect();
   assert_eq!(got_sub, want_sub);
}

#[test]
fn deleting_the_entry_point_keeps_search_alive() {
   let dims = 8;
   let g = HnswGraph::new(DistanceMetric::L2, dims, DEFAULT_M, DEFAULT_EF_CONSTRUCTION);
   for i in 0..50 {
      g.insert(i, &v(i as f32, dims));
   }
   let entry = unpack(g.entry.load(Ordering::Acquire)).0 as usize;
   g.mark_deleted(entry);
   let res = g.search(&v(10.0, dims), 5, 50);
   assert_eq!(res.len(), 5, "search dead after entry deletion");
   assert!(res.iter().all(|r| r.1 != entry));
}

#[test]
fn level_distribution_follows_one_over_m() {
   let g = HnswGraph::new(DistanceMetric::L2, 4, 16, 100);
   let n = 200_000;
   let upper = (0..n).filter(|&i| g.random_level(i) >= 1).count() as f64 / n as f64;
   // P(level >= 1) = 1/M = 0.0625; the old coin flip gave 0.5.
   assert!((upper - 1.0 / 16.0).abs() < 0.005, "P(level>=1) = {upper}");
   let two = (0..n).filter(|&i| g.random_level(i) >= 2).count() as f64 / n as f64;
   assert!((two - 1.0 / 256.0).abs() < 0.002, "P(level>=2) = {two}");
}

#[test]
fn m_param_caps_edge_lists() {
   let dims = 8;
   let g = HnswGraph::new(DistanceMetric::L2, dims, 4, 100);
   for i in 0..400 {
      g.insert(i, &v(i as f32, dims));
   }
   let inner = g.inner.read();
   let mut links = Vec::new();
   for id in 0..inner.len as u32 {
      g.read_links(&inner, id, 0, &mut links);
      assert!(
         links.len() <= 8,
         "level-0 cap 2*M=8 exceeded: {}",
         links.len()
      );
      for level in 1..=inner.level(id) {
         g.read_links(&inner, id, level, &mut links);
         assert!(
            links.len() <= 4,
            "upper-level cap M=4 exceeded: {}",
            links.len()
         );
      }
   }
}

#[test]
fn clustered_recall_with_parallel_build() {
   let mut data = clustered(20_200, 64, 64, 7);
   let queries = data.split_off(20_000);
   let g = build(DistanceMetric::L2, &data, 16, 200);
   assert_eq!(g.linked_len(), data.len());
   let r40 = recall(&g, DistanceMetric::L2, &data, &queries, 40);
   let r200 = recall(&g, DistanceMetric::L2, &data, &queries, 200);
   eprintln!("clustered 20k×64d recall@10: ef=40 {r40:.3}, ef=200 {r200:.3}");
   assert!(r40 >= 0.90, "recall@10 ef=40 = {r40:.3}");
   assert!(r200 >= 0.98, "recall@10 ef=200 = {r200:.3}");
}

#[test]
fn hnsw_recall_5k_uniform_cosine() {
   let dims = 64;
   let mut r = Lcg(12345);
   let data: Vec<Vec<f32>> = (0..5000)
      .map(|_| (0..dims).map(|_| r.next()).collect())
      .collect();
   let g = build(
      DistanceMetric::Cosine,
      &data,
      DEFAULT_M,
      DEFAULT_EF_CONSTRUCTION,
   );
   let queries: Vec<Vec<f32>> = data.iter().step_by(250).cloned().collect();
   let rc = recall(&g, DistanceMetric::Cosine, &data, &queries, 100);
   eprintln!("uniform 5k×64d cosine recall@10 ef=100: {rc:.3}");
   assert!(rc >= 0.95, "recall@10 = {rc:.3}");
}

#[test]
fn progress_reports_every_chunk_and_reaches_total() {
   let data = clustered(10_000, 16, 16, 3);
   let g = HnswGraph::new(DistanceMetric::L2, 16, 16, 100);
   for (i, x) in data.iter().enumerate() {
      g.prepare(i, x);
   }
   let ids: Vec<usize> = (0..data.len()).collect();
   let mut seen = Vec::new();
   g.build_links(&ids, 4, &mut |d| seen.push(d));
   assert!(seen.windows(2).all(|w| w[0] < w[1]));
   assert_eq!(*seen.last().unwrap(), data.len());
   assert_eq!(g.linked_len(), data.len());
}

#[test]
fn compaction_frees_tombstones_and_keeps_recall() {
   let mut data = clustered(8_100, 32, 32, 5);
   let queries = data.split_off(8_000);
   let g = build(DistanceMetric::L2, &data, 16, 200);
   for i in (0..data.len()).step_by(4) {
      g.mark_deleted(i);
   }
   assert_eq!(g.deleted_len(), 2000);
   let freed = g.compact_if_needed();
   assert_eq!(freed, 2000);
   assert_eq!(g.deleted_len(), 0);
   // No live list may point at a freed node.
   {
      let inner = g.inner.read();
      let mut links = Vec::new();
      for id in 0..inner.len as u32 {
         if inner.flag(id) != 0 {
            continue;
         }
         for level in 0..=inner.level(id) {
            g.read_links(&inner, id, level, &mut links);
            assert!(
               links.iter().all(|&e| e % 4 != 0),
               "edge to freed node survived"
            );
         }
      }
   }
   let mut hits = 0;
   for q in &queries {
      let want = exact_topk(DistanceMetric::L2, &data, q, 10, |i| i % 4 == 0);
      let got: Vec<usize> = g.search(q, 10, 100).iter().map(|x| x.1).collect();
      assert!(got.iter().all(|i| i % 4 != 0));
      hits += got.iter().filter(|i| want.contains(i)).count();
   }
   let r = hits as f64 / (queries.len() * 10) as f64;
   assert!(r >= 0.95, "recall after compaction = {r:.3}");
   // Freed ids are handed out again and relink cleanly.
   let ids = g.alloc_ids(10);
   assert!(ids.iter().all(|i| i % 4 == 0 && *i < data.len()));
   for &id in &ids {
      g.prepare(id, &data[id]);
   }
   g.build_links(&ids, 2, &mut |_| {});
   for &id in &ids {
      let top = g.search(&data[id], 1, 64);
      assert_eq!(top[0].1, id, "recycled id {id} not findable");
   }
}

#[test]
fn queries_run_concurrently_with_a_build() {
   use std::sync::Arc;
   let data = Arc::new(clustered(30_000, 32, 32, 11));
   let g = Arc::new(HnswGraph::new(DistanceMetric::L2, 32, 16, 100));
   for (i, x) in data.iter().enumerate() {
      g.prepare(i, x);
   }
   let builder = {
      let g = Arc::clone(&g);
      std::thread::spawn(move || {
         let ids: Vec<usize> = (0..30_000).collect();
         g.build_links(&ids, 4, &mut |_| {});
      })
   };
   let mut answered = 0;
   while !builder.is_finished() {
      let res = g.search(&data[answered % data.len()], 5, 32);
      for (_, id) in &res {
         assert!(*id < data.len());
      }
      answered += 1;
   }
   builder.join().unwrap();
   assert_eq!(g.linked_len(), 30_000);
   assert!(answered > 0);
}

#[test]
fn concurrent_flat_queries_are_batched_and_exact() {
   use std::sync::Arc;
   let data = Arc::new(clustered(20_000, 48, 32, 21));
   let g = Arc::new(HnswGraph::new(DistanceMetric::Cosine, 48, 16, 100));
   for (i, x) in data.iter().enumerate() {
      g.prepare(i, x);
   }
   let handles: Vec<_> = (0..16)
      .map(|t| {
         let g = Arc::clone(&g);
         let data = Arc::clone(&data);
         std::thread::spawn(move || {
            for i in 0..20 {
               let q = &data[(t * 997 + i * 31) % data.len()];
               let want = exact_topk(DistanceMetric::Cosine, &data, q, 7, |_| false);
               let got: Vec<usize> = g.brute_search(q, 7).iter().map(|x| x.1).collect();
               assert_eq!(got, want, "batched exact scan diverged from reference");
            }
         })
      })
      .collect();
   for h in handles {
      h.join().unwrap();
   }
}

/// Regression: ensures concurrent FLAT queries submitted as pool jobs
/// do not deadlock when waiting inside parallel scans.
#[test]
fn flat_queries_as_pool_jobs_do_not_deadlock() {
   use std::sync::Arc;
   let data = Arc::new(clustered(30_000, 32, 16, 5));
   let g = Arc::new(HnswGraph::new(DistanceMetric::L2, 32, 16, 100));
   for (i, x) in data.iter().enumerate() {
      g.prepare(i, x);
   }
   let (tx, rx) = std::sync::mpsc::channel();
   let jobs = crate::pool::global().threads() * 4;
   for i in 0..jobs {
      let (g, data, tx) = (Arc::clone(&g), Arc::clone(&data), tx.clone());
      crate::pool::global().spawn(move || {
         let r = g.brute_search(&data[i * 13], 5);
         tx.send(r[0].1).unwrap();
      });
   }
   for _ in 0..jobs {
      rx.recv_timeout(std::time::Duration::from_secs(30))
         .expect("pool deadlocked");
   }
}

#[test]
fn dump_and_restore_reproduce_search_results() {
   let mut data = clustered(6_100, 24, 24, 9);
   let queries = data.split_off(6_000);
   let g = build(DistanceMetric::L2, &data, 12, 100);
   for i in (0..data.len()).step_by(10) {
      g.mark_deleted(i);
   }
   let dump = g.dump();
   assert_eq!(dump.nodes.len(), data.len() - 600);
   let r = HnswGraph::new(DistanceMetric::L2, 24, 12, 100);
   for n in &dump.nodes {
      r.restore_node(
         n.id as usize,
         &data[n.id as usize],
         n.level as usize,
         &n.links,
      );
   }
   r.restore_entry(dump.entry.map(|e| e as usize));
   assert_eq!(r.linked_len(), g.linked_len());
   for q in &queries {
      let a: Vec<usize> = g.search(q, 10, 64).iter().map(|x| x.1).collect();
      let b: Vec<usize> = r.search(q, 10, 64).iter().map(|x| x.1).collect();
      assert_eq!(a, b, "restored graph answers differently");
   }
}

fn quant_recall(quant: Quant, metric: DistanceMetric, dims: usize) -> (f64, usize, usize) {
   quant_recall_n(quant, metric, dims, 12_000)
}

fn quant_recall_n(
   quant: Quant,
   metric: DistanceMetric,
   dims: usize,
   n: usize,
) -> (f64, usize, usize) {
   let mut data = clustered(n + 200, dims, 48, 17);
   let queries = data.split_off(n);
   let g = HnswGraph::with_quant(metric, dims, 16, 200, quant);
   let sample: Vec<Vec<f32>> = data.iter().step_by(3).cloned().collect();
   g.train(&sample);
   for (i, x) in data.iter().enumerate() {
      g.prepare(i, x);
   }
   let ids: Vec<usize> = (0..data.len()).collect();
   g.build_links(&ids, 8, &mut |_| {});
   let raw_left = g.inner.read().raw.iter().flatten().count();
   let fetch = |id: usize| Some(data[id].clone());
   let mut hits = 0;
   for q in &queries {
      let want = exact_topk(metric, &data, q, 10, |_| false);
      let depth = g.rerank_depth(10, 100);
      let cands = g.search(q, depth, 100);
      let got: Vec<usize> = g.rerank(q, cands, 10, &fetch).iter().map(|x| x.1).collect();
      hits += got.iter().filter(|i| want.contains(i)).count();
   }
   (
      hits as f64 / (queries.len() * 10) as f64,
      g.memory_bytes(),
      raw_left,
   )
}

#[test]
fn int8_graph_recall_and_memory() {
   let f32_mem = {
      let g = build(DistanceMetric::L2, &clustered(12_000, 128, 48, 17), 16, 200);
      g.memory_bytes()
   };
   for metric in [
      DistanceMetric::L2,
      DistanceMetric::Cosine,
      DistanceMetric::Ip,
   ] {
      let (r, mem, raw_left) = quant_recall(Quant::Int8, metric, 128);
      eprintln!(
         "int8 {metric:?} 12k×128d recall@10 after rerank: {r:.3}, {} KiB vs f32 {} KiB",
         mem / 1024,
         f32_mem / 1024
      );
      assert!(r >= 0.97, "{metric:?} int8 recall {r:.3}");
      assert_eq!(raw_left, 0, "raw vectors must be released after the build");
      assert!(
         mem * 2 < f32_mem,
         "int8 graph not smaller: {mem} vs {f32_mem}"
      );
   }
}

/// 1-bit RaBitQ's estimator variance shrinks like 1/√D: it targets
/// high-dimensional embeddings (the paper's 768-1536d regime), not 128d.
#[test]
fn rabitq_graph_recall_after_rerank() {
   for metric in [DistanceMetric::L2, DistanceMetric::Cosine] {
      let (r, mem, _) = quant_recall_n(Quant::RaBitQ, metric, 768, 6_000);
      eprintln!(
         "rabitq {metric:?} 6k×768d recall@10 after rerank: {r:.3}, {} KiB",
         mem / 1024
      );
      assert!(r >= 0.90, "{metric:?} rabitq recall {r:.3}");
   }
}

#[test]
fn quantized_graph_accepts_incremental_inserts() {
   let mut data = clustered(5_300, 64, 16, 23);
   let extra = data.split_off(5_000);
   let g = HnswGraph::with_quant(DistanceMetric::L2, 64, 16, 100, Quant::Int8);
   for (i, x) in data.iter().enumerate() {
      g.prepare(i, x);
   }
   let ids: Vec<usize> = (0..data.len()).collect();
   g.build_links(&ids, 4, &mut |_| {});
   for (j, x) in extra.iter().enumerate() {
      g.insert(5_000 + j, x);
   }
   assert!(g.inner.read().raw.iter().flatten().next().is_none());
   let all: Vec<Vec<f32>> = data.iter().chain(extra.iter()).cloned().collect();
   let fetch = |id: usize| Some(all[id].clone());
   for (j, x) in extra.iter().enumerate().step_by(30) {
      let cands = g.search(x, 30, 64);
      let top = g.rerank(x, cands, 1, &fetch);
      assert_eq!(top[0].1, 5_000 + j, "incrementally inserted node not found");
      assert!(top[0].0 < 1e-4);
   }
   assert!(g.vector_matches(5_000, &extra[0]));
   assert!(!g.vector_matches(5_000, &extra[1]));
}

#[test]
#[ignore = "perf probe: cargo test --release -- --ignored --nocapture probe_quant_speed"]
fn probe_quant_speed() {
   for dims in [128usize, 768] {
      let n = if dims == 128 { 50_000 } else { 20_000 };
      let mut data = clustered(n + 200, dims, 64, 3);
      let queries = data.split_off(n);
      for quant in [Quant::None, Quant::Int8] {
         let g = HnswGraph::with_quant(DistanceMetric::L2, dims, 16, 200, quant);
         for (i, x) in data.iter().enumerate() {
            g.prepare(i, x);
         }
         let ids: Vec<usize> = (0..n).collect();
         g.build_links(&ids, 12, &mut |_| {});
         for ef in [40usize, 160] {
            let t = std::time::Instant::now();
            let mut visited = 0usize;
            for q in &queries {
               visited += g.search(q, 10, ef).len();
            }
            let us = t.elapsed().as_secs_f64() * 1e6 / queries.len() as f64;
            eprintln!("dims={dims} {quant:?} ef={ef}: {us:.1} µs/q ({visited})");
         }
      }
   }
}

#[test]
fn acorn_and_traverse_filters_return_only_members_with_high_recall() {
   let mut data = clustered(20_100, 32, 32, 41);
   let queries = data.split_off(20_000);
   let g = build(DistanceMetric::L2, &data, 16, 200);
   for (pct, acorn) in [(5usize, true), (20, true), (50, false)] {
      let ids: Vec<usize> = (0..data.len()).filter(|i| i % 100 < pct).collect();
      let set = IdSet::from_ids(&ids);
      assert_eq!(set.len(), ids.len());
      let mut hits = 0;
      for q in &queries {
         let want = exact_topk(DistanceMetric::L2, &data, q, 10, |i| i % 100 >= pct);
         let got = g.search_in_set(q, 10, 64, &set, acorn);
         assert!(got.iter().all(|x| set.contains(x.1)), "non-member returned");
         hits += got.iter().filter(|x| want.contains(&x.1)).count();
      }
      let r = hits as f64 / (queries.len() * 10) as f64;
      eprintln!("filter {pct}% acorn={acorn}: recall@10 {r:.3}");
      assert!(r >= 0.93, "{pct}% recall {r:.3}");
   }
}

#[test]
fn range_returns_everything_within_radius() {
   let data = clustered(8_000, 16, 16, 43);
   let g = build(DistanceMetric::L2, &data, 16, 200);
   let q = &data[123];
   let mut all: Vec<f32> = data
      .iter()
      .map(|v| distance(DistanceMetric::L2, q, v))
      .collect();
   all.sort_by(|a, b| a.total_cmp(b));
   let radius = all[300];
   let exact = all.iter().filter(|&&d| d <= radius).count();
   let got = g.range(q, radius, 64, None, true);
   assert!(got.iter().all(|r| r.0 <= radius));
   assert!(
      got.len() as f64 >= 0.95 * exact as f64,
      "range found {} of {exact}",
      got.len()
   );
   let flat = g.range(q, radius, 64, None, false);
   assert_eq!(flat.len(), exact, "exact range must be complete");
}

#[test]
fn reordered_graph_answers_identically_through_the_id_map() {
   let mut data = clustered(10_100, 32, 32, 51);
   let queries = data.split_off(10_000);
   let g = build(DistanceMetric::L2, &data, 16, 150);
   for i in (0..data.len()).step_by(17) {
      g.mark_deleted(i);
   }
   let (r, map) = g.reordered();
   assert_eq!(r.len(), g.len());
   assert!((0..data.len()).step_by(17).all(|i| map[i] == u32::MAX));
   let back: std::collections::HashMap<u32, usize> = map
      .iter()
      .enumerate()
      .filter(|(_, n)| **n != u32::MAX)
      .map(|(o, n)| (*n, o))
      .collect();
   for q in &queries {
      let a: Vec<usize> = g.search(q, 10, 64).iter().map(|x| x.1).collect();
      let b: Vec<usize> = r
         .search(q, 10, 64)
         .iter()
         .map(|x| back[&(x.1 as u32)])
         .collect();
      assert_eq!(a, b);
   }
}

#[test]
#[ignore = "perf probe: cargo test --release -- --ignored --nocapture probe_reorder_speed"]
fn probe_reorder_speed() {
   let n = 200_000;
   let dims = 128;
   let mut data: Vec<Vec<f32>> = clustered(n + 500, dims, 256, 3);
   // Shuffle so storage order carries no locality of its own.
   let mut s = 7u64;
   for i in (1..data.len()).rev() {
      s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
      data.swap(i, (s >> 33) as usize % (i + 1));
   }
   let queries = data.split_off(n);
   let g = build(DistanceMetric::L2, &data, 16, 100);
   let (r, _) = g.reordered();
   for (name, graph) in [("original", &g), ("reordered", &r)] {
      let t = std::time::Instant::now();
      for _ in 0..3 {
         for q in &queries {
            std::hint::black_box(graph.search(q, 10, 64));
         }
      }
      eprintln!(
         "{name}: {:.1} µs/q",
         t.elapsed().as_secs_f64() * 1e6 / (3 * queries.len()) as f64
      );
   }
}
