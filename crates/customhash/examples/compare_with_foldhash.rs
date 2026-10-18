use ahash::RandomState as AHashRandomState;
use customhash::CustomMap;
use dashmap::DashMap;
use foldhash::fast::RandomState as FoldRandomState;
use scc::HashMap as SccMap;
use std::sync::Arc;
use std::time::Instant;

const NUM_THREADS: usize = 16;
const OPS_PER_THREAD: usize = 200_000;
const INITIAL_KEYS: usize = 50_000;

fn main() {
   println!(
      "========================================================================================="
   );
   println!("  EXPERIMENT: scc::HashMap with FoldHash vs AHash vs FyroDB CustomMap");
   println!(
      "  CPU Threads: {} | Keys: {} | Ops/Thread: {} | Total Ops/Test: 3.2M",
      NUM_THREADS, INITIAL_KEYS, OPS_PER_THREAD
   );
   println!(
      "=========================================================================================\n"
   );

   println!("Generating {} test keys...", INITIAL_KEYS);
   let keys: Arc<Vec<String>> = Arc::new(
      (0..INITIAL_KEYS)
         .map(|i| format!("usr_sess_{:010}", i))
         .collect(),
   );

   // 1. scc with Default SipHash
   println!("\n>>> 1. scc::HashMap (Default SipHash) <<<");
   bench_scc(
      "scc (Default SipHash)",
      std::hash::RandomState::new(),
      &keys,
   );

   // 2. scc with AHash
   println!("\n>>> 2. scc::HashMap (AHash) <<<");
   bench_scc("scc (AHash)", AHashRandomState::new(), &keys);

   // 3. scc with FoldHash (The User's Idea!)
   println!("\n>>> 3. scc::HashMap (FoldHash - FyroDB Hasher) 🔥 <<<");
   bench_scc("scc (FoldHash)", FoldRandomState::default(), &keys);

   // 4. DashMap with FoldHash
   println!("\n>>> 4. DashMap (FoldHash) <<<");
   bench_dashmap_foldhash(&keys);

   // 5. FyroDB CustomMap (FoldHash + EBR)
   println!("\n>>> 5. FyroDB CustomMap (FoldHash + EBR) <<<");
   bench_fyro_custommap(&keys);

   println!(
      "\n========================================================================================="
   );
   println!("                                EXPERIMENT COMPLETE");
   println!(
      "========================================================================================="
   );
}

fn bench_scc<H>(name: &str, hasher: H, keys: &Arc<Vec<String>>)
where
   H: std::hash::BuildHasher + Clone + Send + Sync + 'static,
{
   let map = Arc::new(SccMap::<String, u64, H>::with_hasher(hasher.clone()));
   for (i, k) in keys.iter().enumerate() {
      let _ = map.insert_sync(k.clone(), i as u64);
   }

   // 100% Reads
   run_benchmark(&format!("{} - 100% Reads", name), {
      let map = map.clone();
      let keys = keys.clone();
      move |tid| {
         let start = tid * (OPS_PER_THREAD / 2);
         let mut sum = 0u64;
         for i in 0..OPS_PER_THREAD {
            let key = &keys[(start + i) % INITIAL_KEYS];
            if let Some(v) = map.read_sync(key.as_str(), |_, val| *val) {
               sum = sum.wrapping_add(v);
            }
         }
         std::hint::black_box(sum);
      }
   });

   // 90% Read / 10% Write
   run_benchmark(&format!("{} - 90% Read / 10% Write", name), {
      let map = map.clone();
      let keys = keys.clone();
      move |tid| {
         let start = tid * (OPS_PER_THREAD / 2);
         for i in 0..OPS_PER_THREAD {
            let key = &keys[(start + i) % INITIAL_KEYS];
            if i % 10 == 0 {
               let _ = map.upsert_sync(key.clone(), i as u64);
            } else {
               let _ = map.read_sync(key.as_str(), |_, val| *val);
            }
         }
      }
   });

   // 100% Inserts (New Map)
   let map_w = Arc::new(SccMap::<String, u64, H>::with_hasher(hasher));
   run_benchmark(&format!("{} - 100% Inserts", name), {
      let map = map_w.clone();
      move |tid| {
         let base = tid * OPS_PER_THREAD;
         for i in 0..OPS_PER_THREAD {
            let _ = map.insert_sync(format!("ins_scc_{}_{}", tid, i), (base + i) as u64);
         }
      }
   });
}

fn bench_dashmap_foldhash(keys: &Arc<Vec<String>>) {
   let map = Arc::new(DashMap::<String, u64, FoldRandomState>::with_hasher(
      FoldRandomState::default(),
   ));
   for (i, k) in keys.iter().enumerate() {
      map.insert(k.clone(), i as u64);
   }

   run_benchmark("DashMap (FoldHash) - 100% Reads", {
      let map = map.clone();
      let keys = keys.clone();
      move |tid| {
         let start = tid * (OPS_PER_THREAD / 2);
         let mut sum = 0u64;
         for i in 0..OPS_PER_THREAD {
            let key = &keys[(start + i) % INITIAL_KEYS];
            if let Some(v) = map.get(key.as_str()) {
               sum = sum.wrapping_add(*v);
            }
         }
         std::hint::black_box(sum);
      }
   });

   run_benchmark("DashMap (FoldHash) - 90% Read / 10% Write", {
      let map = map.clone();
      let keys = keys.clone();
      move |tid| {
         let start = tid * (OPS_PER_THREAD / 2);
         for i in 0..OPS_PER_THREAD {
            let key = &keys[(start + i) % INITIAL_KEYS];
            if i % 10 == 0 {
               map.insert(key.clone(), i as u64);
            } else {
               let _ = map.get(key.as_str());
            }
         }
      }
   });

   let map_w = Arc::new(DashMap::<String, u64, FoldRandomState>::with_hasher(
      FoldRandomState::default(),
   ));
   run_benchmark("DashMap (FoldHash) - 100% Inserts", {
      let map = map_w.clone();
      move |tid| {
         let base = tid * OPS_PER_THREAD;
         for i in 0..OPS_PER_THREAD {
            map.insert(format!("ins_d_f_{}_{}", tid, i), (base + i) as u64);
         }
      }
   });
}

fn bench_fyro_custommap(keys: &Arc<Vec<String>>) {
   let map = Arc::new(CustomMap::<u64>::with_capacity(64, INITIAL_KEYS * 2));
   for (i, k) in keys.iter().enumerate() {
      map.insert(k.clone(), i as u64);
   }

   run_benchmark("FyroDB CustomMap - 100% Reads", {
      let map = map.clone();
      let keys = keys.clone();
      move |tid| {
         let start = tid * (OPS_PER_THREAD / 2);
         let mut sum = 0u64;
         for i in 0..OPS_PER_THREAD {
            let key = &keys[(start + i) % INITIAL_KEYS];
            if let Some(v) = map.get(key.as_str()) {
               sum = sum.wrapping_add(v);
            }
         }
         std::hint::black_box(sum);
      }
   });

   run_benchmark("FyroDB CustomMap - 90% Read / 10% Write", {
      let map = map.clone();
      let keys = keys.clone();
      move |tid| {
         let start = tid * (OPS_PER_THREAD / 2);
         for i in 0..OPS_PER_THREAD {
            let key = &keys[(start + i) % INITIAL_KEYS];
            if i % 10 == 0 {
               map.insert(key.clone(), i as u64);
            } else {
               let _ = map.get(key.as_str());
            }
         }
      }
   });

   let map_w = Arc::new(CustomMap::<u64>::with_capacity(
      64,
      NUM_THREADS * OPS_PER_THREAD,
   ));
   run_benchmark("FyroDB CustomMap - 100% Inserts", {
      let map = map_w.clone();
      move |tid| {
         let base = tid * OPS_PER_THREAD;
         for i in 0..OPS_PER_THREAD {
            map.insert(format!("ins_fy_{}_{}", tid, i), (base + i) as u64);
         }
      }
   });
}

fn run_benchmark<F>(name: &str, task_factory: F)
where
   F: Fn(usize) + Send + Sync + 'static + Clone,
{
   let total_ops = NUM_THREADS * OPS_PER_THREAD;
   let mut handles = Vec::with_capacity(NUM_THREADS);

   let start = Instant::now();
   for thread_id in 0..NUM_THREADS {
      let task = task_factory.clone();
      handles.push(std::thread::spawn(move || {
         task(thread_id);
      }));
   }

   for h in handles {
      h.join().unwrap();
   }
   let elapsed = start.elapsed();
   let elapsed_secs = elapsed.as_secs_f64();
   let mops = (total_ops as f64 / 1_000_000.0) / elapsed_secs;
   let ns_per_op = (elapsed.as_nanos() as f64) / (total_ops as f64);

   println!(
      "  {:<45} | Time: {:>7.2?} | {:>7.2} M ops/sec | {:>6.2} ns/op",
      name, elapsed, mops, ns_per_op
   );
}
