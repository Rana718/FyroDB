use ahash::RandomState as AHashBuild;
use foldhash::fast::RandomState as FoldHashBuild;
use std::hash::{BuildHasher, Hasher};
use std::time::Instant;

const ITERATIONS: usize = 10_000_000;

fn main() {
   println!("==========================================================================");
   println!("               PURE HASH FUNCTION MICRO-BENCHMARK");
   println!("      Comparing: Default SipHash vs AHash vs FoldHash (FyroDB)");
   println!("      Total Hashes: 10,000,000 strings (short & medium)");
   println!("==========================================================================\n");

   let keys: Vec<String> = (0..10_000).map(|i| format!("user_session_{}", i)).collect();

   // 1. Default SipHash
   let sip = std::collections::hash_map::RandomState::new();
   let start = Instant::now();
   let mut sum1 = 0u64;
   for i in 0..ITERATIONS {
      let k = &keys[i % keys.len()];
      let mut hasher = sip.build_hasher();
      std::hash::Hash::hash(k, &mut hasher);
      sum1 = sum1.wrapping_add(hasher.finish());
   }
   let sip_elapsed = start.elapsed();
   let sip_ns = sip_elapsed.as_nanos() as f64 / ITERATIONS as f64;
   let sip_mops = (ITERATIONS as f64 / 1_000_000.0) / sip_elapsed.as_secs_f64();
   std::hint::black_box(sum1);

   // 2. AHash
   let ahash = AHashBuild::new();
   let start = Instant::now();
   let mut sum2 = 0u64;
   for i in 0..ITERATIONS {
      let k = &keys[i % keys.len()];
      sum2 = sum2.wrapping_add(ahash.hash_one(k));
   }
   let ahash_elapsed = start.elapsed();
   let ahash_ns = ahash_elapsed.as_nanos() as f64 / ITERATIONS as f64;
   let ahash_mops = (ITERATIONS as f64 / 1_000_000.0) / ahash_elapsed.as_secs_f64();
   std::hint::black_box(sum2);

   // 3. FoldHash (FyroDB)
   let fold = FoldHashBuild::default();
   let start = Instant::now();
   let mut sum3 = 0u64;
   for i in 0..ITERATIONS {
      let k = &keys[i % keys.len()];
      sum3 = sum3.wrapping_add(fold.hash_one(k));
   }
   let fold_elapsed = start.elapsed();
   let fold_ns = fold_elapsed.as_nanos() as f64 / ITERATIONS as f64;
   let fold_mops = (ITERATIONS as f64 / 1_000_000.0) / fold_elapsed.as_secs_f64();
   std::hint::black_box(sum3);

   println!(
      "{:<28} | {:<12} | {:<18} | {:<10}",
      "Hash Function", "Time", "Throughput", "Latency"
   );
   println!("--------------------------------------------------------------------------");
   println!(
      "{:<28} | {:>10.2?} | {:>10.2} M hash/s | {:>6.2} ns/hash",
      "1. Default SipHash (std)", sip_elapsed, sip_mops, sip_ns
   );
   println!(
      "{:<28} | {:>10.2?} | {:>10.2} M hash/s | {:>6.2} ns/hash",
      "2. AHash (Hardware AES)", ahash_elapsed, ahash_mops, ahash_ns
   );
   println!(
      "{:<28} | {:>10.2?} | {:>10.2} M hash/s | {:>6.2} ns/hash",
      "3. FoldHash (FyroDB Hasher)", fold_elapsed, fold_mops, fold_ns
   );
   println!("==========================================================================\n");
}
