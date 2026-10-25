/// Standalone benchmark: std HashMap (SipHash) vs foldhash FixedState vs CustomMap
/// Simulates fyro-dbcache cache-key access patterns: prefixed 32-char hex string keys.
/// Usage: cargo run --release --bin hashmap_bench

use customhash::CustomMap;
use foldhash::fast::FixedState;
use std::collections::HashMap;
use std::time::Instant;

const N: usize = 200_000;
const LOOKUPS: usize = 5_000_000;

fn make_key(i: usize) -> String {
   format!("q:users:{:016x}", i as u64)
}

fn bench<F: Fn() -> u64>(label: &str, f: F) {
   // warm up
   let _ = f();
   let _ = f();
   let t = Instant::now();
   let iters = 10;
   let mut total = 0u64;
   for _ in 0..iters {
      total += f();
   }
   let elapsed = t.elapsed();
   let ops_per_sec = (LOOKUPS as f64 * iters as f64) / elapsed.as_secs_f64();
   println!(
      "{:<38} {:>10.2} Mops/s   checksum={total}",
      label,
      ops_per_sec / 1_000_000.0
   );
}

fn mem_kb<F: FnMut() -> usize>(label: &str, mut f: F) {
   let before = allocated_kb();
   let size = f();
   let after = allocated_kb();
   let used = after.saturating_sub(before);
   println!("  {:<36} ~{used} KB heap  ({size} entries)", label);
}

fn allocated_kb() -> usize {
   // Read /proc/self/status VmRSS as a proxy for heap usage
   if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
      for line in s.lines() {
         if line.starts_with("VmRSS:") {
            let kb: usize = line
               .split_whitespace()
               .nth(1)
               .and_then(|v| v.parse().ok())
               .unwrap_or(0);
            return kb;
         }
      }
   }
   0
}

fn main() {
   println!("Building key sets ({N} entries, {LOOKUPS} lookups each)...\n");

   let keys: Vec<String> = (0..N).map(make_key).collect();
   let miss_keys: Vec<String> = (N..N + N / 10).map(make_key).collect();

   // ── 1. std HashMap (SipHash) ──────────────────────────────────────────────
   let mut std_map: HashMap<String, u64> = HashMap::with_capacity(N);
   for (i, k) in keys.iter().enumerate() {
      std_map.insert(k.clone(), i as u64);
   }

   bench("std HashMap (SipHash) — hit", || {
      let mut sum = 0u64;
      for k in keys.iter().take(LOOKUPS % N + 1).cycle().take(LOOKUPS) {
         sum = sum.wrapping_add(*std_map.get(k.as_str()).unwrap_or(&0));
      }
      sum
   });
   bench("std HashMap (SipHash) — miss", || {
      let mut sum = 0u64;
      let miss = &miss_keys;
      for k in miss.iter().cycle().take(LOOKUPS) {
         sum = sum.wrapping_add(std_map.get(k.as_str()).map(|v| *v).unwrap_or(1));
      }
      sum
   });

   // ── 2. foldhash FixedState ────────────────────────────────────────────────
   let mut fold_map: HashMap<String, u64, FixedState> =
      HashMap::with_capacity_and_hasher(N, FixedState::with_seed(0));
   for (i, k) in keys.iter().enumerate() {
      fold_map.insert(k.clone(), i as u64);
   }

   bench("foldhash FixedState    — hit", || {
      let mut sum = 0u64;
      for k in keys.iter().take(LOOKUPS % N + 1).cycle().take(LOOKUPS) {
         sum = sum.wrapping_add(*fold_map.get(k.as_str()).unwrap_or(&0));
      }
      sum
   });
   bench("foldhash FixedState    — miss", || {
      let mut sum = 0u64;
      let miss = &miss_keys;
      for k in miss.iter().cycle().take(LOOKUPS) {
         sum = sum.wrapping_add(fold_map.get(k.as_str()).map(|v| *v).unwrap_or(1));
      }
      sum
   });

   // Build helper so each variant gets its own independent map + warmup.
   let make_custom = || {
      let c: CustomMap<u64> = CustomMap::with_capacity(128, N);
      for (i, k) in keys.iter().enumerate() { c.insert_str(k, i as u64); }
      c
   };

   // ── 3. CustomMap pinned — server reality ──────────────────────────────────
   // sock.rs holds one pin() per command-dispatch loop; all lookups pay zero
   // EBR atomic ops. Uses table_relaxed() + Relaxed probe loads.
   {
      let custom_p = make_custom();
      let guard = customhash::pin();
      bench("CustomMap (pinned)     — hit", || {
         let mut sum = 0u64;
         for k in keys.iter().take(LOOKUPS % N + 1).cycle().take(LOOKUPS) {
            sum = sum.wrapping_add(custom_p.get_pinned(k, &guard).unwrap_or(0));
         }
         sum
      });
      bench("CustomMap (pinned)     — miss", || {
         let mut sum = 0u64;
         for k in miss_keys.iter().cycle().take(LOOKUPS) {
            sum = sum.wrapping_add(custom_p.get_pinned(k, &guard).unwrap_or(1));
         }
         sum
      });
   }

   // ── 4. CustomMap unpinned — worst case (background/one-shot threads) ──────
   {
      let custom_u = make_custom();
      bench("CustomMap (unpinned)   — hit", || {
         let mut sum = 0u64;
         for k in keys.iter().take(LOOKUPS % N + 1).cycle().take(LOOKUPS) {
            sum = sum.wrapping_add(custom_u.get(k).unwrap_or(0));
         }
         sum
      });
      bench("CustomMap (unpinned)   — miss", || {
         let mut sum = 0u64;
         for k in miss_keys.iter().cycle().take(LOOKUPS) {
            sum = sum.wrapping_add(custom_u.get(k).unwrap_or(1));
         }
         sum
      });
   }

   // ── Memory comparison ─────────────────────────────────────────────────────
   println!("\nMemory footprint ({N} String keys, u64 values):");
   {
      let mut m: HashMap<String, u64> = HashMap::with_capacity(N);
      mem_kb("std HashMap (SipHash)", || {
         for (i, k) in keys.iter().enumerate() { m.insert(k.clone(), i as u64); }
         m.len()
      });
   }
   {
      let mut m: HashMap<String, u64, FixedState> =
         HashMap::with_capacity_and_hasher(N, FixedState::with_seed(0));
      mem_kb("foldhash FixedState", || {
         for (i, k) in keys.iter().enumerate() { m.insert(k.clone(), i as u64); }
         m.len()
      });
   }
   {
      let cm: CustomMap<u64> = CustomMap::with_capacity(128, N);
      mem_kb("CustomMap (lock-free)", || {
         for (i, k) in keys.iter().enumerate() { cm.insert_str(k, i as u64); }
         cm.len()
      });
   }
}
