use customhash::{CustomMap, force_collect};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Custom tracking allocator to measure exact heap bytes allocated.
struct TrackingAlloc;
static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for TrackingAlloc {
   unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
      let ptr = unsafe { System.alloc(layout) };
      if !ptr.is_null() {
         ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
      }
      ptr
   }

   unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
      unsafe { System.dealloc(ptr, layout) };
      ALLOCATED.fetch_sub(layout.size(), Ordering::Relaxed);
   }

   unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
      let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
      if !new_ptr.is_null() {
         if new_size > layout.size() {
            ALLOCATED.fetch_add(new_size - layout.size(), Ordering::Relaxed);
         } else {
            ALLOCATED.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
         }
      }
      new_ptr
   }
}

#[global_allocator]
static GLOBAL_ALLOC: TrackingAlloc = TrackingAlloc;

pub fn heap_allocated_bytes() -> usize {
   ALLOCATED.load(Ordering::Relaxed)
}

/// Memory statistics read directly from tracking allocator and Linux `/proc/self/status` / `/proc/self/statm`.
#[derive(Debug, Clone, Copy, Default)]
pub struct MemoryInfo {
   /// Exact heap allocated bytes tracked by allocator
   pub heap_bytes: usize,
   /// Resident Set Size (physical memory in bytes)
   pub rss_bytes: usize,
   /// Anonymous Resident Memory (actual heap & stack in bytes, excluding file pages)
   pub rss_anon_bytes: usize,
   /// Virtual Memory Size in bytes
   pub vm_size_bytes: usize,
   /// Peak Virtual Memory in bytes
   pub vm_peak_bytes: usize,
}

impl MemoryInfo {
   pub fn snapshot() -> Self {
      let mut info = Self::default();
      info.heap_bytes = heap_allocated_bytes();

      if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
         for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
               if let Some(kb) = parse_kb(rest) {
                  info.rss_bytes = kb * 1024;
               }
            } else if let Some(rest) = line.strip_prefix("RssAnon:") {
               if let Some(kb) = parse_kb(rest) {
                  info.rss_anon_bytes = kb * 1024;
               }
            } else if let Some(rest) = line.strip_prefix("VmSize:") {
               if let Some(kb) = parse_kb(rest) {
                  info.vm_size_bytes = kb * 1024;
               }
            } else if let Some(rest) = line.strip_prefix("VmPeak:") {
               if let Some(kb) = parse_kb(rest) {
                  info.vm_peak_bytes = kb * 1024;
               }
            }
         }
      }

      // Fallback to /proc/self/statm if status was incomplete
      if info.rss_bytes == 0 {
         if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
            if let Some(pages) = statm
               .split_whitespace()
               .nth(1)
               .and_then(|s| s.parse::<usize>().ok())
            {
               info.rss_bytes = pages * 4096;
            }
         }
      }

      info
   }

   pub fn heap_mb(&self) -> f64 {
      self.heap_bytes as f64 / (1024.0 * 1024.0)
   }

   pub fn rss_mb(&self) -> f64 {
      self.rss_bytes as f64 / (1024.0 * 1024.0)
   }

   pub fn rss_anon_mb(&self) -> f64 {
      self.rss_anon_bytes as f64 / (1024.0 * 1024.0)
   }
}

fn parse_kb(s: &str) -> Option<usize> {
   s.trim_start()
      .split_whitespace()
      .next()?
      .parse::<usize>()
      .ok()
}

/// Latency and throughput benchmark statistics.
pub struct BenchResult {
   pub name: String,
   pub operations: usize,
   pub elapsed: Duration,
   pub ops_per_sec: f64,
   pub min_latency_ns: u64,
   pub avg_latency_ns: f64,
   pub p50_latency_ns: u64,
   pub p90_latency_ns: u64,
   pub p99_latency_ns: u64,
   pub p999_latency_ns: u64,
   pub max_latency_ns: u64,
   pub start_mem: MemoryInfo,
   pub end_mem: MemoryInfo,
   pub delta_heap_bytes: isize,
   pub delta_rss_bytes: isize,
   pub heap_bytes_per_key: f64,
   pub rss_bytes_per_key: f64,
}

impl BenchResult {
   pub fn display_summary(&self) {
      let delta_heap_mb = self.delta_heap_bytes as f64 / (1024.0 * 1024.0);
      let delta_rss_mb = self.delta_rss_bytes as f64 / (1024.0 * 1024.0);
      println!(
         "┌─────────────────────────────────────────────────────────────────────────────────────────────┐"
      );
      println!("│ Benchmark: {:<80} │", self.name);
      println!(
         "├─────────────────────────────────────────────────────────────────────────────────────────────┤"
      );
      println!(
         "│ Operations: {:>10} ops  │ Elapsed: {:>10.3?} │ Throughput: {:>10.2} M ops/s │",
         format_count(self.operations),
         self.elapsed,
         self.ops_per_sec / 1_000_000.0
      );
      println!(
         "├─────────────────────────────────────────────────────────────────────────────────────────────┤"
      );
      println!(
         "│ Latency (ns): Min: {:>6} │ Avg: {:>8.1} │ p50: {:>6} │ p90: {:>6} │ p99: {:>6} │ Max: {:>7} │",
         self.min_latency_ns,
         self.avg_latency_ns,
         self.p50_latency_ns,
         self.p90_latency_ns,
         self.p99_latency_ns,
         self.max_latency_ns
      );
      println!(
         "├─────────────────────────────────────────────────────────────────────────────────────────────┤"
      );
      println!(
         "│ Heap RAM:  Start: {:>7.2} MB │ End: {:>7.2} MB │ Delta: {:>+7.2} MB │ Heap/Key: {:>7.1} B │",
         self.start_mem.heap_mb(),
         self.end_mem.heap_mb(),
         delta_heap_mb,
         self.heap_bytes_per_key
      );
      println!(
         "│ RSS (OS):  Start: {:>7.2} MB │ End: {:>7.2} MB │ Delta: {:>+7.2} MB │  RSS/Key: {:>7.1} B │",
         self.start_mem.rss_mb(),
         self.end_mem.rss_mb(),
         delta_rss_mb,
         self.rss_bytes_per_key
      );
      println!(
         "└─────────────────────────────────────────────────────────────────────────────────────────────┘"
      );
   }
}

fn format_count(n: usize) -> String {
   let s = n.to_string();
   let mut out = String::new();
   let mut count = 0;
   for c in s.chars().rev() {
      if count > 0 && count % 3 == 0 {
         out.push(',');
      }
      out.push(c);
      count += 1;
   }
   out.chars().rev().collect()
}

fn calculate_percentiles(mut samples: Vec<u64>) -> (u64, u64, u64, u64, u64, u64) {
   if samples.is_empty() {
      return (0, 0, 0, 0, 0, 0);
   }
   samples.sort_unstable();
   let min = samples[0];
   let max = samples[samples.len() - 1];
   let p50 = samples[samples.len() * 50 / 100];
   let p90 = samples[samples.len() * 90 / 100];
   let p99 = samples[samples.len() * 99 / 100];
   let p999 = samples[(samples.len() * 999 / 1000).min(samples.len() - 1)];
   (min, p50, p90, p99, p999, max)
}

/// Run full sequential GET/SET benchmarks.
fn bench_sequential_get_set(num_keys: usize, shard_count: usize) {
   println!(
      "\n═══════════════════════════════════════════════════════════════════════════════════════════"
   );
   println!(
      "  PART 1 — Single-Threaded GET / SET Benchmark ({} keys, {} shards)",
      format_count(num_keys),
      shard_count
   );
   println!(
      "═══════════════════════════════════════════════════════════════════════════════════════════\n"
   );

   force_collect();
   let baseline_mem = MemoryInfo::snapshot();
   println!(
      "Baseline Process RAM: Heap: {:.2} MB, RSS: {:.2} MB\n",
      baseline_mem.heap_mb(),
      baseline_mem.rss_mb()
   );

   let map = CustomMap::<String>::with_capacity(shard_count, num_keys);
   force_collect();
   let prealloc_mem = MemoryInfo::snapshot();
   let prealloc_heap_delta =
      (prealloc_mem.heap_bytes as isize) - (baseline_mem.heap_bytes as isize);
   let prealloc_rss_delta = (prealloc_mem.rss_bytes as isize) - (baseline_mem.rss_bytes as isize);
   println!(
      "After Pre-allocating Map ({} shards for {} expected keys):",
      shard_count,
      format_count(num_keys)
   );
   println!(
      "  Heap: {:.2} MB (Slot table allocation delta: {:+.2} MB)",
      prealloc_mem.heap_mb(),
      prealloc_heap_delta as f64 / (1024.0 * 1024.0)
   );
   println!(
      "  RSS:  {:.2} MB (Physical memory delta:        {:+.2} MB)\n",
      prealloc_mem.rss_mb(),
      prealloc_rss_delta as f64 / (1024.0 * 1024.0)
   );

   // 1. SET (Insert New Keys)
   let sample_rate = (num_keys / 100_000).max(1);
   let mut latencies = Vec::with_capacity(num_keys / sample_rate + 1);

   let start_mem = MemoryInfo::snapshot();
   let start_time = Instant::now();

   for i in 0..num_keys {
      let k = format!("key:{i:08}");
      let v = format!("val:{i:08}");

      if i % sample_rate == 0 {
         let t0 = Instant::now();
         let inserted = map.insert(k, v);
         let elapsed_ns = t0.elapsed().as_nanos() as u64;
         latencies.push(elapsed_ns);
         assert!(inserted);
      } else {
         let inserted = map.insert(k, v);
         assert!(inserted);
      }
   }

   let elapsed = start_time.elapsed();
   force_collect();
   let end_mem = MemoryInfo::snapshot();

   let total_lat_sum: u64 = latencies.iter().sum();
   let avg_lat = total_lat_sum as f64 / latencies.len() as f64;
   let (min_lat, p50_lat, p90_lat, p99_lat, p999_lat, max_lat) = calculate_percentiles(latencies);

   let delta_heap = (end_mem.heap_bytes as isize) - (start_mem.heap_bytes as isize);
   let delta_rss = (end_mem.rss_bytes as isize) - (start_mem.rss_bytes as isize);
   let heap_bytes_per_key = delta_heap as f64 / num_keys as f64;
   let rss_bytes_per_key = delta_rss as f64 / num_keys as f64;
   let ops_per_sec = num_keys as f64 / elapsed.as_secs_f64();

   let set_bench = BenchResult {
      name: format!(
         "SET (Insert {} unique keys: String -> String)",
         format_count(num_keys)
      ),
      operations: num_keys,
      elapsed,
      ops_per_sec,
      min_latency_ns: min_lat,
      avg_latency_ns: avg_lat,
      p50_latency_ns: p50_lat,
      p90_latency_ns: p90_lat,
      p99_latency_ns: p99_lat,
      p999_latency_ns: p999_lat,
      max_latency_ns: max_lat,
      start_mem,
      end_mem,
      delta_heap_bytes: delta_heap,
      delta_rss_bytes: delta_rss,
      heap_bytes_per_key,
      rss_bytes_per_key,
   };
   set_bench.display_summary();

   assert_eq!(map.len(), num_keys);

   // 2. GET (Hit 100% - Read Existing Keys)
   let mut get_latencies = Vec::with_capacity(num_keys / sample_rate + 1);
   let start_mem = MemoryInfo::snapshot();
   let start_time = Instant::now();

   for i in 0..num_keys {
      let k = format!("key:{i:08}");

      if i % sample_rate == 0 {
         let t0 = Instant::now();
         let val = map.get(&k);
         let elapsed_ns = t0.elapsed().as_nanos() as u64;
         get_latencies.push(elapsed_ns);
         assert!(val.is_some());
      } else {
         let val = map.get(&k);
         assert!(val.is_some());
      }
   }

   let elapsed = start_time.elapsed();
   force_collect();
   let end_mem = MemoryInfo::snapshot();

   let total_lat_sum: u64 = get_latencies.iter().sum();
   let avg_lat = total_lat_sum as f64 / get_latencies.len() as f64;
   let (min_lat, p50_lat, p90_lat, p99_lat, p999_lat, max_lat) =
      calculate_percentiles(get_latencies);

   let delta_heap = (end_mem.heap_bytes as isize) - (start_mem.heap_bytes as isize);
   let delta_rss = (end_mem.rss_bytes as isize) - (start_mem.rss_bytes as isize);
   let ops_per_sec = num_keys as f64 / elapsed.as_secs_f64();

   let get_bench = BenchResult {
      name: format!(
         "GET (Hit 100% lookup of {} keys - cloned String)",
         format_count(num_keys)
      ),
      operations: num_keys,
      elapsed,
      ops_per_sec,
      min_latency_ns: min_lat,
      avg_latency_ns: avg_lat,
      p50_latency_ns: p50_lat,
      p90_latency_ns: p90_lat,
      p99_latency_ns: p99_lat,
      p999_latency_ns: p999_lat,
      max_latency_ns: max_lat,
      start_mem,
      end_mem,
      delta_heap_bytes: delta_heap,
      delta_rss_bytes: delta_rss,
      heap_bytes_per_key: 0.0,
      rss_bytes_per_key: 0.0,
   };
   get_bench.display_summary();

   // 3. GET_REF / ZERO-CLONE (Zero-copy read via ValueRef)
   let mut get_ref_latencies = Vec::with_capacity(num_keys / sample_rate + 1);
   let start_mem = MemoryInfo::snapshot();
   let start_time = Instant::now();

   for i in 0..num_keys {
      let k = format!("key:{i:08}");

      if i % sample_rate == 0 {
         let t0 = Instant::now();
         let val_ref = map.get_ref(&k);
         let elapsed_ns = t0.elapsed().as_nanos() as u64;
         get_ref_latencies.push(elapsed_ns);
         assert!(val_ref.is_some());
      } else {
         let val_ref = map.get_ref(&k);
         assert!(val_ref.is_some());
      }
   }

   let elapsed = start_time.elapsed();
   force_collect();
   let end_mem = MemoryInfo::snapshot();

   let total_lat_sum: u64 = get_ref_latencies.iter().sum();
   let avg_lat = total_lat_sum as f64 / get_ref_latencies.len() as f64;
   let (min_lat, p50_lat, p90_lat, p99_lat, p999_lat, max_lat) =
      calculate_percentiles(get_ref_latencies);

   let delta_heap = (end_mem.heap_bytes as isize) - (start_mem.heap_bytes as isize);
   let delta_rss = (end_mem.rss_bytes as isize) - (start_mem.rss_bytes as isize);
   let ops_per_sec = num_keys as f64 / elapsed.as_secs_f64();

   let get_ref_bench = BenchResult {
      name: format!(
         "GET_REF (Zero-clone reference lookup of {} keys)",
         format_count(num_keys)
      ),
      operations: num_keys,
      elapsed,
      ops_per_sec,
      min_latency_ns: min_lat,
      avg_latency_ns: avg_lat,
      p50_latency_ns: p50_lat,
      p90_latency_ns: p90_lat,
      p99_latency_ns: p99_lat,
      p999_latency_ns: p999_lat,
      max_latency_ns: max_lat,
      start_mem,
      end_mem,
      delta_heap_bytes: delta_heap,
      delta_rss_bytes: delta_rss,
      heap_bytes_per_key: 0.0,
      rss_bytes_per_key: 0.0,
   };
   get_ref_bench.display_summary();

   // 4. SET (Overwrite / Update Existing Keys)
   let mut update_latencies = Vec::with_capacity(num_keys / sample_rate + 1);
   let start_mem = MemoryInfo::snapshot();
   let start_time = Instant::now();

   for i in 0..num_keys {
      let k = format!("key:{i:08}");
      let v = format!("updated_val:{i:08}");

      if i % sample_rate == 0 {
         let t0 = Instant::now();
         let is_new = map.set(&k, v, || k.clone());
         let elapsed_ns = t0.elapsed().as_nanos() as u64;
         update_latencies.push(elapsed_ns);
         assert!(!is_new); // Existing key, returns false for is_new
      } else {
         let is_new = map.set(&k, v, || k.clone());
         assert!(!is_new);
      }
   }

   let elapsed = start_time.elapsed();
   force_collect();
   let end_mem = MemoryInfo::snapshot();

   let total_lat_sum: u64 = update_latencies.iter().sum();
   let avg_lat = total_lat_sum as f64 / update_latencies.len() as f64;
   let (min_lat, p50_lat, p90_lat, p99_lat, p999_lat, max_lat) =
      calculate_percentiles(update_latencies);

   let delta_heap = (end_mem.heap_bytes as isize) - (start_mem.heap_bytes as isize);
   let delta_rss = (end_mem.rss_bytes as isize) - (start_mem.rss_bytes as isize);
   let ops_per_sec = num_keys as f64 / elapsed.as_secs_f64();

   let update_bench = BenchResult {
      name: format!(
         "SET Overwrite (Update {} existing keys in-place)",
         format_count(num_keys)
      ),
      operations: num_keys,
      elapsed,
      ops_per_sec,
      min_latency_ns: min_lat,
      avg_latency_ns: avg_lat,
      p50_latency_ns: p50_lat,
      p90_latency_ns: p90_lat,
      p99_latency_ns: p99_lat,
      p999_latency_ns: p999_lat,
      max_latency_ns: max_lat,
      start_mem,
      end_mem,
      delta_heap_bytes: delta_heap,
      delta_rss_bytes: delta_rss,
      heap_bytes_per_key: 0.0,
      rss_bytes_per_key: 0.0,
   };
   update_bench.display_summary();

   // 5. GET (Miss 100% - Non-existent Keys)
   let mut miss_latencies = Vec::with_capacity(num_keys / sample_rate + 1);
   let start_mem = MemoryInfo::snapshot();
   let start_time = Instant::now();

   for i in 0..num_keys {
      let k = format!("missing_key:{i:08}");

      if i % sample_rate == 0 {
         let t0 = Instant::now();
         let val = map.get(&k);
         let elapsed_ns = t0.elapsed().as_nanos() as u64;
         miss_latencies.push(elapsed_ns);
         assert!(val.is_none());
      } else {
         let val = map.get(&k);
         assert!(val.is_none());
      }
   }

   let elapsed = start_time.elapsed();
   force_collect();
   let end_mem = MemoryInfo::snapshot();

   let total_lat_sum: u64 = miss_latencies.iter().sum();
   let avg_lat = total_lat_sum as f64 / miss_latencies.len() as f64;
   let (min_lat, p50_lat, p90_lat, p99_lat, p999_lat, max_lat) =
      calculate_percentiles(miss_latencies);

   let delta_heap = (end_mem.heap_bytes as isize) - (start_mem.heap_bytes as isize);
   let delta_rss = (end_mem.rss_bytes as isize) - (start_mem.rss_bytes as isize);
   let ops_per_sec = num_keys as f64 / elapsed.as_secs_f64();

   let miss_bench = BenchResult {
      name: format!(
         "GET (Miss 100% lookup of {} non-existent keys)",
         format_count(num_keys)
      ),
      operations: num_keys,
      elapsed,
      ops_per_sec,
      min_latency_ns: min_lat,
      avg_latency_ns: avg_lat,
      p50_latency_ns: p50_lat,
      p90_latency_ns: p90_lat,
      p99_latency_ns: p99_lat,
      p999_latency_ns: p999_lat,
      max_latency_ns: max_lat,
      start_mem,
      end_mem,
      delta_heap_bytes: delta_heap,
      delta_rss_bytes: delta_rss,
      heap_bytes_per_key: 0.0,
      rss_bytes_per_key: 0.0,
   };
   miss_bench.display_summary();
}

/// Run multi-threaded concurrent GET/SET benchmarks.
fn bench_concurrent_get_set(total_keys: usize, threads: usize, shard_count: usize) {
   println!(
      "\n═══════════════════════════════════════════════════════════════════════════════════════════"
   );
   println!(
      "  PART 2 — Multi-Threaded Concurrent GET / SET Benchmark ({} threads, {} keys, {} shards)",
      threads,
      format_count(total_keys),
      shard_count
   );
   println!(
      "═══════════════════════════════════════════════════════════════════════════════════════════\n"
   );

   let map = Arc::new(CustomMap::<String>::with_capacity(shard_count, total_keys));
   let keys_per_thread = total_keys / threads;

   // 1. Concurrent Multi-Threaded SET
   force_collect();
   let start_mem = MemoryInfo::snapshot();
   let start_time = Instant::now();

   std::thread::scope(|scope| {
      for t in 0..threads {
         let map = Arc::clone(&map);
         scope.spawn(move || {
            let start_idx = t * keys_per_thread;
            let end_idx = if t == threads - 1 {
               total_keys
            } else {
               start_idx + keys_per_thread
            };
            for i in start_idx..end_idx {
               let k = format!("ckey:{i:08}");
               let v = format!("cval:{i:08}");
               map.insert(k, v);
            }
         });
      }
   });

   let elapsed = start_time.elapsed();
   force_collect();
   let end_mem = MemoryInfo::snapshot();

   let delta_heap = (end_mem.heap_bytes as isize) - (start_mem.heap_bytes as isize);
   let delta_rss = (end_mem.rss_bytes as isize) - (start_mem.rss_bytes as isize);
   let heap_bytes_per_key = delta_heap as f64 / total_keys as f64;
   let rss_bytes_per_key = delta_rss as f64 / total_keys as f64;
   let ops_per_sec = total_keys as f64 / elapsed.as_secs_f64();

   let concurrent_set_bench = BenchResult {
      name: format!(
         "Concurrent SET ({} threads parallel insert of {} keys)",
         threads,
         format_count(total_keys)
      ),
      operations: total_keys,
      elapsed,
      ops_per_sec,
      min_latency_ns: 0,
      avg_latency_ns: (elapsed.as_nanos() as f64 / total_keys as f64) * threads as f64,
      p50_latency_ns: 0,
      p90_latency_ns: 0,
      p99_latency_ns: 0,
      p999_latency_ns: 0,
      max_latency_ns: 0,
      start_mem,
      end_mem,
      delta_heap_bytes: delta_heap,
      delta_rss_bytes: delta_rss,
      heap_bytes_per_key,
      rss_bytes_per_key,
   };
   concurrent_set_bench.display_summary();

   assert_eq!(map.len(), total_keys);

   // 2. Concurrent Multi-Threaded GET
   force_collect();
   let start_mem = MemoryInfo::snapshot();
   let start_time = Instant::now();

   std::thread::scope(|scope| {
      for t in 0..threads {
         let map = Arc::clone(&map);
         scope.spawn(move || {
            let start_idx = t * keys_per_thread;
            let end_idx = if t == threads - 1 {
               total_keys
            } else {
               start_idx + keys_per_thread
            };
            for i in start_idx..end_idx {
               let k = format!("ckey:{i:08}");
               let val = map.get(&k);
               assert!(val.is_some());
            }
         });
      }
   });

   let elapsed = start_time.elapsed();
   force_collect();
   let end_mem = MemoryInfo::snapshot();

   let delta_heap = (end_mem.heap_bytes as isize) - (start_mem.heap_bytes as isize);
   let delta_rss = (end_mem.rss_bytes as isize) - (start_mem.rss_bytes as isize);
   let ops_per_sec = total_keys as f64 / elapsed.as_secs_f64();

   let concurrent_get_bench = BenchResult {
      name: format!(
         "Concurrent GET ({} threads parallel read of {} keys)",
         threads,
         format_count(total_keys)
      ),
      operations: total_keys,
      elapsed,
      ops_per_sec,
      min_latency_ns: 0,
      avg_latency_ns: (elapsed.as_nanos() as f64 / total_keys as f64) * threads as f64,
      p50_latency_ns: 0,
      p90_latency_ns: 0,
      p99_latency_ns: 0,
      p999_latency_ns: 0,
      max_latency_ns: 0,
      start_mem,
      end_mem,
      delta_heap_bytes: delta_heap,
      delta_rss_bytes: delta_rss,
      heap_bytes_per_key: 0.0,
      rss_bytes_per_key: 0.0,
   };
   concurrent_get_bench.display_summary();

   // 3. Concurrent Mixed Workload (90% GET / 10% SET)
   force_collect();
   let start_mem = MemoryInfo::snapshot();
   let start_time = Instant::now();

   std::thread::scope(|scope| {
      for t in 0..threads {
         let map = Arc::clone(&map);
         scope.spawn(move || {
            let start_idx = t * keys_per_thread;
            let end_idx = if t == threads - 1 {
               total_keys
            } else {
               start_idx + keys_per_thread
            };
            let count = end_idx - start_idx;
            for i in 0..count {
               let key_id = start_idx + (i % count);
               let k = format!("ckey:{key_id:08}");
               if i % 10 == 0 {
                  // 10% SET / Overwrite
                  let v = format!("newval:{i:08}");
                  map.set(&k, v, || k.clone());
               } else {
                  // 90% GET
                  let _ = map.get(&k);
               }
            }
         });
      }
   });

   let elapsed = start_time.elapsed();
   force_collect();
   let end_mem = MemoryInfo::snapshot();

   let delta_heap = (end_mem.heap_bytes as isize) - (start_mem.heap_bytes as isize);
   let delta_rss = (end_mem.rss_bytes as isize) - (start_mem.rss_bytes as isize);
   let ops_per_sec = total_keys as f64 / elapsed.as_secs_f64();

   let concurrent_mixed_bench = BenchResult {
      name: format!(
         "Concurrent Mixed 90% GET / 10% SET ({} threads, {} total ops)",
         threads,
         format_count(total_keys)
      ),
      operations: total_keys,
      elapsed,
      ops_per_sec,
      min_latency_ns: 0,
      avg_latency_ns: (elapsed.as_nanos() as f64 / total_keys as f64) * threads as f64,
      p50_latency_ns: 0,
      p90_latency_ns: 0,
      p99_latency_ns: 0,
      p999_latency_ns: 0,
      max_latency_ns: 0,
      start_mem,
      end_mem,
      delta_heap_bytes: delta_heap,
      delta_rss_bytes: delta_rss,
      heap_bytes_per_key: 0.0,
      rss_bytes_per_key: 0.0,
   };
   concurrent_mixed_bench.display_summary();
}

/// Compare memory usage for Inline keys (≤ 15 bytes) vs Heap keys (> 15 bytes).
fn audit_key_size_memory_comparison(count: usize) {
   println!(
      "\n═══════════════════════════════════════════════════════════════════════════════════════════"
   );
   println!(
      "  PART 3 — RAM Usage & Memory Footprint Deep Dive ({} keys per test)",
      format_count(count)
   );
   println!(
      "═══════════════════════════════════════════════════════════════════════════════════════════\n"
   );

   // Case A: Inline short keys (≤ 15 bytes, e.g. "k:00000001" is 10 chars)
   force_collect();
   let before_a = MemoryInfo::snapshot();
   let map_inline = CustomMap::<u64>::with_capacity(32, count);
   for i in 0..count {
      map_inline.insert(format!("k:{i:08}"), i as u64);
   }
   force_collect();
   let after_a = MemoryInfo::snapshot();
   let delta_heap_a = (after_a.heap_bytes as isize) - (before_a.heap_bytes as isize);
   let delta_rss_a = (after_a.rss_bytes as isize) - (before_a.rss_bytes as isize);
   let heap_per_key_a = delta_heap_a as f64 / count as f64;
   let rss_per_key_a = delta_rss_a as f64 / count as f64;

   println!("  [Test A] CompactKey Inline (≤ 15 bytes string keys, u64 value):");
   println!("    Sample Key: 'k:00000001' (10 bytes inline, 0 heap alloc for key)");
   println!(
      "    Heap: {:.2} MB → {:.2} MB (Delta: {:+.2} MB)",
      before_a.heap_mb(),
      after_a.heap_mb(),
      delta_heap_a as f64 / (1024.0 * 1024.0)
   );
   println!(
      "    RSS:  {:.2} MB → {:.2} MB (Delta: {:+.2} MB)",
      before_a.rss_mb(),
      after_a.rss_mb(),
      delta_rss_a as f64 / (1024.0 * 1024.0)
   );
   println!(
      "    Exact Heap RAM per key: {:.1} bytes / key",
      heap_per_key_a
   );
   println!(
      "    Physical RSS per key:   {:.1} bytes / key",
      rss_per_key_a
   );
   println!();

   // Case B: Long keys (> 15 bytes, e.g. "long_prefix_namespace_user_id:00000001" is 39 chars)
   force_collect();
   let before_b = MemoryInfo::snapshot();
   let map_long = CustomMap::<u64>::with_capacity(32, count);
   for i in 0..count {
      map_long.insert(format!("long_prefix_namespace_user_id:{i:08}"), i as u64);
   }
   force_collect();
   let after_b = MemoryInfo::snapshot();
   let delta_heap_b = (after_b.heap_bytes as isize) - (before_b.heap_bytes as isize);
   let delta_rss_b = (after_b.rss_bytes as isize) - (before_b.rss_bytes as isize);
   let heap_per_key_b = delta_heap_b as f64 / count as f64;
   let rss_per_key_b = delta_rss_b as f64 / count as f64;

   println!("  [Test B] CompactKey Heap Allocated (> 15 bytes string keys, u64 value):");
   println!("    Sample Key: 'long_prefix_namespace_user_id:00000001' (39 bytes on heap)");
   println!(
      "    Heap: {:.2} MB → {:.2} MB (Delta: {:+.2} MB)",
      before_b.heap_mb(),
      after_b.heap_mb(),
      delta_heap_b as f64 / (1024.0 * 1024.0)
   );
   println!(
      "    RSS:  {:.2} MB → {:.2} MB (Delta: {:+.2} MB)",
      before_b.rss_mb(),
      after_b.rss_mb(),
      delta_rss_b as f64 / (1024.0 * 1024.0)
   );
   println!(
      "    Exact Heap RAM per key: {:.1} bytes / key",
      heap_per_key_b
   );
   println!(
      "    Physical RSS per key:   {:.1} bytes / key",
      rss_per_key_b
   );
   println!(
      "    Heap String Overhead:   {:+.1} bytes / key",
      heap_per_key_b - heap_per_key_a
   );
   println!();
}

#[test]
fn test_get_set() {
   let num_keys = 500_000;
   let shards = 64;
   let threads = std::thread::available_parallelism()
      .map(|n| n.get())
      .unwrap_or(4);

   println!(
      "\n╔═══════════════════════════════════════════════════════════════════════════════════════════╗"
   );
   println!(
      "║             CUSTOM HASHMAP (customhash::CustomMap) GET/SET BENCHMARK & RAM AUDIT          ║"
   );
   println!(
      "╚═══════════════════════════════════════════════════════════════════════════════════════════╝"
   );
   println!("  System CPUs: {}", threads);
   println!("  Test Keys:   {}", format_count(num_keys));
   println!("  Shards:      {}", shards);

   bench_sequential_get_set(num_keys, shards);
   bench_concurrent_get_set(num_keys, threads, shards);
   audit_key_size_memory_comparison(num_keys / 2);

   println!(
      "\n═══════════════════════════════════════════════════════════════════════════════════════════"
   );
   println!(
      "  ALL GET / SET BENCHMARKS & MEMORY AUDITS COMPLETED SUCCESSFULLY!                          "
   );
   println!(
      "═══════════════════════════════════════════════════════════════════════════════════════════\n"
   );
}
