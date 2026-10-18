//! Micro-benchmarks for isolated hot paths: RESP parsing, keyspace GET/SET,
//! distance kernels, and HNSW. Run with: cargo bench --bench core

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use fyro_db::search::DistanceMetric;
use fyro_db::storage::hnsw::HnswGraph;
use fyro_db::storage::simd;
use fyro_db::storage::store::Store;
use fyro_db::utils::parser::{ParseResult, RespParser};
use std::hint::black_box;

fn lcg(seed: &mut u64) -> f32 {
   *seed = seed
      .wrapping_mul(6364136223846793005)
      .wrapping_add(1442695040888963407);
   ((*seed >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
}

fn vectors(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
   let mut s = seed;
   // Clustered: 64 centroids + noise, closer to real embeddings than
   // uniform noise (whose neighbors are all equidistant).
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

fn bench_parser(c: &mut Criterion) {
   let mut g = c.benchmark_group("resp_parse");
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
   g.throughput(Throughput::Elements(128));
   g.bench_function("pipeline_128_set", |b| {
      b.iter_batched_ref(
         || {
            let mut p = RespParser::new();
            let buf = p.read_buf();
            let _ = buf;
            while p.rbuf.len() < wire.len() + 1024 {
               p.rbuf.resize(p.rbuf.len() * 2 + 4096, 0);
            }
            p.rbuf[..wire.len()].copy_from_slice(&wire);
            p.did_fill(wire.len());
            p
         },
         |p| {
            let mut n = 0;
            while let ParseResult::Complete = p.parse_one() {
               n += p.parts_raw.len();
            }
            black_box(n)
         },
         BatchSize::SmallInput,
      )
   });
   g.finish();
}

fn bench_keyspace(c: &mut Criterion) {
   let mut g = c.benchmark_group("keyspace");
   let store = Store::new();
   let keys: Vec<String> = (0..200_000).map(|i| format!("user:{i:08}")).collect();
   for k in &keys {
      store.set_string(k, "0123456789abcdef", 0);
   }
   let mut out = Vec::with_capacity(64);
   let mut i = 0usize;
   g.bench_function("get_hit_200k", |b| {
      b.iter(|| {
         i = (i + 7919) % keys.len();
         out.clear();
         black_box(store.get_to_buf(&keys[i], &mut out))
      })
   });
   g.bench_function("set_overwrite_200k", |b| {
      b.iter(|| {
         i = (i + 7919) % keys.len();
         store.set_string(&keys[i], "fedcba9876543210", 0)
      })
   });
   g.finish();
}

fn bench_kernels(c: &mut Criterion) {
   let mut g = c.benchmark_group("distance");
   for dim in [128usize, 768, 1536] {
      let v = vectors(2, dim, 42);
      g.throughput(Throughput::Bytes((dim * 8) as u64));
      g.bench_with_input(BenchmarkId::new("dot_f32", dim), &dim, |b, _| {
         b.iter(|| black_box(simd::dot_f32(black_box(&v[0]), black_box(&v[1]))))
      });
      g.bench_with_input(BenchmarkId::new("l2_f32", dim), &dim, |b, _| {
         b.iter(|| black_box(simd::l2_f32(black_box(&v[0]), black_box(&v[1]))))
      });
   }
   g.finish();
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

fn bench_hnsw(c: &mut Criterion) {
   let mut g = c.benchmark_group("hnsw");
   g.sample_size(10);
   g.bench_function("build_20k_128d", |b| {
      b.iter(|| black_box(build(20_000, 128).0.len()))
   });
   let (graph, _) = build(50_000, 128);
   let queries = vectors(256, 128, 99);
   let mut qi = 0usize;
   for ef in [10usize, 64, 256] {
      g.bench_with_input(
         BenchmarkId::new("search_50k_128d_k10", ef),
         &ef,
         |b, &ef| {
            b.iter(|| {
               qi = (qi + 1) % queries.len();
               black_box(graph.search(&queries[qi], 10, ef))
            })
         },
      );
   }
   g.bench_function("flat_50k_128d_k10", |b| {
      b.iter(|| {
         qi = (qi + 1) % queries.len();
         black_box(graph.brute_search(&queries[qi], 10))
      })
   });
   g.finish();
}

criterion_group!(
   benches,
   bench_parser,
   bench_keyspace,
   bench_kernels,
   bench_hnsw
);
criterion_main!(benches);
