use customhash::{CustomMap, pin};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[global_allocator]
static GLOBAL: rust_zmalloc::Zmalloc = rust_zmalloc::Zmalloc;

const WORKERS: usize = 12;

struct BenchMetric {
   name: &'static str,
   total_ops: usize,
   elapsed: Duration,
}

impl BenchMetric {
   fn ns_per_op(&self) -> f64 {
      self.elapsed.as_nanos() as f64 / self.total_ops as f64
   }

   fn fmt_throughput(&self) -> String {
      let rate = self.total_ops as f64 / self.elapsed.as_secs_f64();
      if rate >= 1e9 {
         format!("{:.2} B ops/sec", rate / 1e9)
      } else if rate >= 1e6 {
         format!("{:.2} M ops/sec", rate / 1e6)
      } else if rate >= 1e3 {
         format!("{:.1} k ops/sec", rate / 1e3)
      } else {
         format!("{:.0} ops/sec", rate)
      }
   }
}

fn print_table_header(title: &str, keyspace: usize, ops_per_worker: usize) {
   println!("\n╔═══════════════════════════════════════════════════════════════════════════════════════════════════╗");
   println!("║ {:<97} ║", format!("{} (Keys: {}, Workers: 12)", title, format_num(keyspace)));
   println!("║ Workload: 12 Threads (6 Cores / 12 Hardware Threads) | Ops/Worker: {:<29} ║", format_num(ops_per_worker));
   println!("╠══════════════════════════════════════════╦══════════════╦════════════╦═════════════════╦════════════╣");
   println!("║ Benchmark Phase                          ║ Total Ops    ║ Elapsed    ║ Throughput      ║ Latency    ║");
   println!("╠══════════════════════════════════════════╬══════════════╬════════════╬═════════════════╬════════════╣");
}

fn print_metric_row(m: &BenchMetric) {
   println!(
      "║ {:<40} ║ {:>12} ║ {:>8.2?} ║ {:>15} ║ {:>7.1} ns ║",
      m.name,
      format_num(m.total_ops),
      m.elapsed,
      m.fmt_throughput(),
      m.ns_per_op()
   );
}

fn print_table_footer() {
   println!("╚══════════════════════════════════════════╩══════════════╩════════════╩═════════════════╩════════════╝");
}

fn format_num(n: usize) -> String {
   let s = n.to_string();
   let mut out = String::new();
   let len = s.len();
   for (i, c) in s.chars().enumerate() {
      if i > 0 && (len - i).is_multiple_of(3) {
         out.push(',');
      }
      out.push(c);
   }
   out
}

fn run_benchmark_phase(keyspace_size: usize, ops_per_worker: usize) {
   let total_read_ops = WORKERS * ops_per_worker;
   let title = format!("CUSTOMHASH BENCHMARK: {} KEYSPACE", format_num(keyspace_size));
   print_table_header(&title, keyspace_size, ops_per_worker);

   // Pre-generate unique string keys for deterministic, zero-allocation benchmarking
   let keys: Arc<Vec<String>> = Arc::new(
      (0..keyspace_size)
         .map(|i| format!("usr_k_{:010}", i))
         .collect(),
   );

   let missing_keys: Arc<Vec<String>> = Arc::new(
      (0..keyspace_size.min(100_000))
         .map(|i| format!("miss_k_{:010}", i))
         .collect(),
   );

   // Map sized with 128 shards and pre-allocated capacity matching keyspace
   let map = Arc::new(CustomMap::<u64>::with_capacity(128, keyspace_size * 2));

   // 1. Concurrent Fresh Inserts (0 -> N keys across 12 workers)
   let m_insert = {
      let map = map.clone();
      let keys = keys.clone();
      let chunk_size = keyspace_size / WORKERS;
      let start = Instant::now();
      std::thread::scope(|s| {
         for tid in 0..WORKERS {
            let map = map.clone();
            let keys = keys.clone();
            s.spawn(move || {
               let from = tid * chunk_size;
               let to = if tid == WORKERS - 1 { keyspace_size } else { from + chunk_size };
               for i in from..to {
                  map.insert(keys[i].clone(), i as u64);
               }
            });
         }
      });
      BenchMetric {
         name: "1. Concurrent Inserts (Fresh 0->N)",
         total_ops: keyspace_size,
         elapsed: start.elapsed(),
      }
   };
   print_metric_row(&m_insert);

   // 2. 100% Reads (Point Get)
   let m_get = {
      let map = map.clone();
      let keys = keys.clone();
      let start = Instant::now();
      std::thread::scope(|s| {
         for tid in 0..WORKERS {
            let map = map.clone();
            let keys = keys.clone();
            s.spawn(move || {
               let mut sum = 0u64;
               let offset = tid * 104729; // distinct prime stride
               for i in 0..ops_per_worker {
                  let idx = (offset + i) % keyspace_size;
                  if let Some(v) = map.get(&keys[idx]) {
                     sum = sum.wrapping_add(v);
                  }
               }
               std::hint::black_box(sum);
            });
         }
      });
      BenchMetric {
         name: "2. 100% Reads (Point Get .get())",
         total_ops: total_read_ops,
         elapsed: start.elapsed(),
      }
   };
   print_metric_row(&m_get);

   // 3. 100% Reads Zero-Copy Reference (.get_ref() with EBR ValueRef)
   let m_get_ref = {
      let map = map.clone();
      let keys = keys.clone();
      let start = Instant::now();
      std::thread::scope(|s| {
         for tid in 0..WORKERS {
            let map = map.clone();
            let keys = keys.clone();
            s.spawn(move || {
               let mut sum = 0u64;
               let offset = tid * 104729;
               for i in 0..ops_per_worker {
                  let idx = (offset + i) % keyspace_size;
                  if let Some(v) = map.get_ref(&keys[idx]) {
                     sum = sum.wrapping_add(*v);
                  }
               }
               std::hint::black_box(sum);
            });
         }
      });
      BenchMetric {
         name: "3. 100% Reads Zero-Copy (.get_ref())",
         total_ops: total_read_ops,
         elapsed: start.elapsed(),
      }
   };
   print_metric_row(&m_get_ref);

   // 4. Pipelined Batch Prefetch (64 keys per batch with hardware prefetch & pinned EBR)
   let m_prefetch = {
      let map = map.clone();
      let keys = keys.clone();
      const BATCH: usize = 64;
      let start = Instant::now();
      std::thread::scope(|s| {
         for tid in 0..WORKERS {
            let map = map.clone();
            let keys = keys.clone();
            s.spawn(move || {
               let _guard = pin();
               let mut sum = 0u64;
               let offset = tid * 104729;
               let mut batch_keys: [&str; BATCH] = [""; BATCH];
               let mut hashes = [0u64; BATCH];
               let mut tables = [std::ptr::null(); BATCH];

               for b in (0..ops_per_worker).step_by(BATCH) {
                  let n = BATCH.min(ops_per_worker - b);
                  for j in 0..n {
                     batch_keys[j] = &keys[(offset + b + j) % keyspace_size];
                  }
                  map.prefetch_slots(&batch_keys[..n], &mut hashes[..n], &mut tables[..n]);
                  for j in 0..n {
                     let h = hashes[j];
                     let idx = map.shard_of(h);
                     let found = map.with_entry_pinned(batch_keys[j], h, idx, |val| {
                        sum = sum.wrapping_add(*val);
                     });
                     std::hint::black_box(found);
                  }
               }
               std::hint::black_box(sum);
            });
         }
      });
      BenchMetric {
         name: "4. Pipelined Prefetch (64-Batch Pinned)",
         total_ops: total_read_ops,
         elapsed: start.elapsed(),
      }
   };
   print_metric_row(&m_prefetch);

   // 5. 100% In-Place Overwrites (.set() updating existing records)
   let m_overwrite = {
      let map = map.clone();
      let keys = keys.clone();
      let start = Instant::now();
      std::thread::scope(|s| {
         for tid in 0..WORKERS {
            let map = map.clone();
            let keys = keys.clone();
            s.spawn(move || {
               let offset = tid * 104729;
               for i in 0..ops_per_worker {
                  let idx = (offset + i) % keyspace_size;
                  map.set(&keys[idx], (i + 1) as u64, || keys[idx].clone());
               }
            });
         }
      });
      BenchMetric {
         name: "5. 100% Overwrites (In-Place Update)",
         total_ops: total_read_ops,
         elapsed: start.elapsed(),
      }
   };
   print_metric_row(&m_overwrite);

   // 6. Mixed 90% Read / 10% Write (Realistic Production Cache Profile)
   let m_mix90 = {
      let map = map.clone();
      let keys = keys.clone();
      let start = Instant::now();
      std::thread::scope(|s| {
         for tid in 0..WORKERS {
            let map = map.clone();
            let keys = keys.clone();
            s.spawn(move || {
               let offset = tid * 104729;
               let mut sum = 0u64;
               for i in 0..ops_per_worker {
                  let idx = (offset + i) % keyspace_size;
                  if i % 10 == 0 {
                     map.set(&keys[idx], i as u64, || keys[idx].clone());
                  } else if let Some(v) = map.get(&keys[idx]) {
                     sum = sum.wrapping_add(v);
                  }
               }
               std::hint::black_box(sum);
            });
         }
      });
      BenchMetric {
         name: "6. Mixed 90% Read / 10% Write",
         total_ops: total_read_ops,
         elapsed: start.elapsed(),
      }
   };
   print_metric_row(&m_mix90);

   // 7. Mixed 50% Read / 50% Write (Heavy Write Contention)
   let m_mix50 = {
      let map = map.clone();
      let keys = keys.clone();
      let start = Instant::now();
      std::thread::scope(|s| {
         for tid in 0..WORKERS {
            let map = map.clone();
            let keys = keys.clone();
            s.spawn(move || {
               let offset = tid * 104729;
               let mut sum = 0u64;
               for i in 0..ops_per_worker {
                  let idx = (offset + i) % keyspace_size;
                  if (i & 1) == 0 {
                     map.set(&keys[idx], (i * 3) as u64, || keys[idx].clone());
                  } else if let Some(v) = map.get(&keys[idx]) {
                     sum = sum.wrapping_add(v);
                  }
               }
               std::hint::black_box(sum);
            });
         }
      });
      BenchMetric {
         name: "7. Mixed 50% Read / 50% Write",
         total_ops: total_read_ops,
         elapsed: start.elapsed(),
      }
   };
   print_metric_row(&m_mix50);

   // 8. 100% Read Miss (Lookup non-existent keys)
   let m_miss = {
      let map = map.clone();
      let mkeys = missing_keys.clone();
      let mlen = mkeys.len();
      let start = Instant::now();
      std::thread::scope(|s| {
         for tid in 0..WORKERS {
            let map = map.clone();
            let mkeys = mkeys.clone();
            s.spawn(move || {
               let offset = tid * 7919;
               for i in 0..ops_per_worker {
                  let idx = (offset + i) % mlen;
                  let res = map.get(&mkeys[idx]);
                  std::hint::black_box(res);
               }
            });
         }
      });
      BenchMetric {
         name: "8. 100% Read Miss (Non-existent)",
         total_ops: total_read_ops,
         elapsed: start.elapsed(),
      }
   };
   print_metric_row(&m_miss);

   // 9. Concurrent Deletes (Remove all keys across 12 workers)
   let m_delete = {
      let map = map.clone();
      let keys = keys.clone();
      let chunk_size = keyspace_size / WORKERS;
      let start = Instant::now();
      std::thread::scope(|s| {
         for tid in 0..WORKERS {
            let map = map.clone();
            let keys = keys.clone();
            s.spawn(move || {
               let from = tid * chunk_size;
               let to = if tid == WORKERS - 1 { keyspace_size } else { from + chunk_size };
               for i in from..to {
                  map.remove_no_clone(&keys[i]);
               }
            });
         }
      });
      BenchMetric {
         name: "9. Concurrent Deletes (.remove())",
         total_ops: keyspace_size,
         elapsed: start.elapsed(),
      }
   };
   print_metric_row(&m_delete);

   print_table_footer();
   println!("  Final Map Size: {} keys (all cleaned up cleanly)\n", map.len());
}

fn main() {
   println!("\n╔═══════════════════════════════════════════════════════════════════════════════════════════════════╗");
   println!("║                 🔥 FYRODB CUSTOMHASH MULTI-CORE BENCHMARK (12 WORKERS / 6 CORES) 🔥               ║");
   println!("║ Architecture: Lockless Sharded Robin-Hood/Linear-Probe + Fast FoldHash + Wait-Free EBR            ║");
   println!("╚═══════════════════════════════════════════════════════════════════════════════════════════════════╝");

   // Phase 1: 50,000 Keys (50k)
   println!("\n>>> STARTING PHASE 1: 50,000 KEYS (50k) <<<");
   run_benchmark_phase(50_000, 1_000_000); // 12 workers * 1M ops = 12,000,000 ops per workload

   // Phase 2: 2,000,000 Keys (2M)
   println!("\n>>> STARTING PHASE 2: 2,000,000 KEYS (2M) <<<");
   run_benchmark_phase(2_000_000, 500_000); // 12 workers * 500k ops = 6,000,000 ops per workload

   println!("=====================================================================================================");
   println!("                                    BENCHMARK COMPLETE");
   println!("=====================================================================================================\n");
}
