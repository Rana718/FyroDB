//! Micro-benchmarks for isolated hot paths: RESP parsing, keyspace GET/SET,
//! distance kernels, and HNSW. Run with: cargo bench --bench core

use fyro_db::search::DistanceMetric;
use fyro_db::storage::hnsw::HnswGraph;
use fyro_db::storage::simd;
use fyro_db::storage::store::Store;
use fyro_db::utils::parser::{ParseResult, RespParser};
use std::hint::black_box;
use std::time::{Duration, Instant};

fn lcg(seed: &mut u64) -> f32 {
   *seed = seed
      .wrapping_mul(6364136223846793005)
      .wrapping_add(1442695040888963407);
   ((*seed >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
}

fn vectors(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
   let mut s = seed;
   let cents: Vec<Vec<f32>> = (0..64)
      .map(|_| (0..dim).map(|_| lcg(&mut s)).collect())
      .collect();
   (0..n)
      .map(|i| {
         cents[i % 64]
            .iter()
            .map(|c| c + 0.25 * lcg(&mut s))
            .collect()
      })
      .collect()
}

fn fmt_rate(rate: f64) -> String {
   if rate >= 1e9 {
      format!("{:.2}B ops/sec", rate / 1e9)
   } else if rate >= 1e6 {
      format!("{:.2}M ops/sec", rate / 1e6)
   } else if rate >= 1e3 {
      format!("{:.1}k ops/sec", rate / 1e3)
   } else {
      format!("{:.0} ops/sec", rate)
   }
}

fn print_bench(label: &str, ops: u64, elapsed: Duration) {
   let rate = ops as f64 / elapsed.as_secs_f64();
   println!(
      "  {:<36} {:>10} {:>16}",
      label,
      format!("{}ms", elapsed.as_millis()),
      fmt_rate(rate)
   );
}

fn bench_parser() {
   let mut wire = Vec::new();
   for i in 0..128 {
      let key = format!("key:{i}");
      wire.extend_from_slice(
         format!(
            "*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n$16\r\n0123456789abcdef\r\n",
            key.len()
         )
         .as_bytes(),
      );
   }

   const ITERS: usize = 10_000;
   let mut parsers = Vec::with_capacity(ITERS);
   for _ in 0..ITERS {
      let mut p = RespParser::new();
      let _ = p.read_buf();
      while p.rbuf.len() < wire.len() + 1024 {
         p.rbuf.resize(p.rbuf.len() * 2 + 4096, 0);
      }
      p.rbuf[..wire.len()].copy_from_slice(&wire);
      p.did_fill(wire.len());
      parsers.push(p);
   }

   let total_cmds = (ITERS * 128) as u64;
   let start = Instant::now();
   for p in &mut parsers {
      let mut n = 0;
      while let ParseResult::Complete = p.parse_one() {
         n += p.parts_raw.len();
      }
      black_box(n);
   }
   print_bench("RESP pipeline 128 SET parse", total_cmds, start.elapsed());
}

fn bench_keyspace() {
   let store = Store::new();
   let keys: Vec<String> = (0..200_000).map(|i| format!("user:{i:08}")).collect();
   for k in &keys {
      store.set_string(k, "0123456789abcdef", 0);
   }
   let mut out = Vec::with_capacity(64);
   let mut i = 0usize;

   const GET_OPS: u64 = 2_000_000;
   let start = Instant::now();
   for _ in 0..GET_OPS {
      i = (i + 7919) % keys.len();
      out.clear();
      black_box(store.get_to_buf(&keys[i], &mut out));
   }
   print_bench("keyspace GET (hit 200k)", GET_OPS, start.elapsed());

   const SET_OPS: u64 = 1_000_000;
   let start = Instant::now();
   for _ in 0..SET_OPS {
      i = (i + 7919) % keys.len();
      store.set_string(&keys[i], "fedcba9876543210", 0);
   }
   print_bench("keyspace SET overwrite (200k)", SET_OPS, start.elapsed());
}

fn bench_kernels() {
   for dim in [128usize, 768, 1536] {
      let v = vectors(2, dim, 42);
      const OPS: u64 = 500_000;

      let start = Instant::now();
      for _ in 0..OPS {
         black_box(simd::dot_f32(black_box(&v[0]), black_box(&v[1])));
      }
      print_bench(&format!("simd::dot_f32 (dim={dim})"), OPS, start.elapsed());

      let start = Instant::now();
      for _ in 0..OPS {
         black_box(simd::l2_f32(black_box(&v[0]), black_box(&v[1])));
      }
      print_bench(&format!("simd::l2_f32 (dim={dim})"), OPS, start.elapsed());
   }
}

fn build(n: usize, dim: usize) -> (HnswGraph, Vec<Vec<f32>>) {
   let data = vectors(n, dim, 7);
   let g = HnswGraph::new(DistanceMetric::L2, dim, 16, 200);
   let ids: Vec<usize> = (0..n).collect();
   for (i, v) in data.iter().enumerate() {
      g.prepare(i, v);
   }
   g.build_links(&ids, num_cpus::get(), &mut |_| {});
   (g, data)
}

fn bench_hnsw() {
   let start = Instant::now();
   let (graph, _) = build(20_000, 128);
   print_bench("HNSW build (20k 128d)", 20_000, start.elapsed());
   black_box(graph.len());

   let (graph50, _) = build(50_000, 128);
   let queries = vectors(256, 128, 99);
   let mut qi = 0usize;

   for ef in [10usize, 64, 256] {
      const SEARCH_OPS: u64 = 2_000;
      let start = Instant::now();
      for _ in 0..SEARCH_OPS {
         qi = (qi + 1) % queries.len();
         black_box(graph50.search(&queries[qi], 10, ef));
      }
      print_bench(
         &format!("HNSW search 50k 128d (ef={ef})"),
         SEARCH_OPS,
         start.elapsed(),
      );
   }

   const FLAT_OPS: u64 = 100;
   let start = Instant::now();
   for _ in 0..FLAT_OPS {
      qi = (qi + 1) % queries.len();
      black_box(graph50.brute_search(&queries[qi], 10));
   }
   print_bench("HNSW flat brute-force 50k", FLAT_OPS, start.elapsed());
}

fn main() {
   println!("============================================================");
   println!("           FyroDB Core Micro-Benchmarks                     ");
   println!("============================================================");
   println!("── Summary ─────────────────────────────────────────────────────");
   println!("  Benchmark                               Elapsed       Throughput");
   println!("  ──────────────────────────────────── ────────── ────────────────");

   bench_parser();
   bench_keyspace();
   bench_kernels();
   bench_hnsw();
}
