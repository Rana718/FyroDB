use std::sync::Arc;
use std::time::Instant;
use ahash::RandomState;
use customhash::CustomMap;
use dashmap::DashMap;
use scc::HashMap as SccMap;

const NUM_THREADS: usize = 16;
const OPS_PER_THREAD: usize = 200_000;
const INITIAL_KEYS: usize = 50_000;

fn main() {
    println!("=========================================================================================");
    println!("  CONCURRENT HASHMAP SHOWDOWN: DashMap vs scc::HashMap vs FyroDB CustomMap");
    println!("  Workload: String Keys (<= 23B inline) | CPU Threads: {} | Keys: {} | Ops/Thread: {}", 
             NUM_THREADS, INITIAL_KEYS, OPS_PER_THREAD);
    println!("=========================================================================================\n");

    // Pre-generate initial keys so lookups don't spend time allocating strings
    println!("Generating {} test keys...", INITIAL_KEYS);
    let initial_keys: Arc<Vec<String>> = Arc::new(
        (0..INITIAL_KEYS)
            .map(|i| format!("usr_sess_{:010}", i))
            .collect()
    );

    println!("\n>>> 1. Benchmarking: DashMap (Default SipHash) <<<");
    bench_dashmap_default(&initial_keys);

    println!("\n>>> 2. Benchmarking: DashMap (AHash) <<<");
    bench_dashmap_ahash(&initial_keys);

    println!("\n>>> 3. Benchmarking: scc::HashMap (Default Hasher) <<<");
    bench_scc_default(&initial_keys);

    println!("\n>>> 4. Benchmarking: scc::HashMap (AHash) <<<");
    bench_scc_ahash(&initial_keys);

    println!("\n>>> 5. Benchmarking: FyroDB CustomMap (FoldHash + EBR) <<<");
    bench_fyro_custommap(&initial_keys);

    println!("\n=========================================================================================");
    println!("                               BENCHMARK COMPLETE");
    println!("=========================================================================================");
}

// -----------------------------------------------------------------------------
// 1. DashMap (Default)
// -----------------------------------------------------------------------------
fn bench_dashmap_default(keys: &Arc<Vec<String>>) {
    let map = Arc::new(DashMap::<String, u64>::new());
    for (i, k) in keys.iter().enumerate() {
        map.insert(k.clone(), i as u64);
    }

    // 100% Reads
    run_benchmark("DashMap (Default) - 100% Reads", {
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

    // 90% Read / 10% Write
    run_benchmark("DashMap (Default) - 90% Read / 10% Write", {
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

    // 100% Inserts
    let map_w = Arc::new(DashMap::<String, u64>::new());
    run_benchmark("DashMap (Default) - 100% Inserts", {
        let map = map_w.clone();
        move |tid| {
            let base = tid * OPS_PER_THREAD;
            for i in 0..OPS_PER_THREAD {
                map.insert(format!("ins_d_{}_{}", tid, i), (base + i) as u64);
            }
        }
    });
}

// -----------------------------------------------------------------------------
// 2. DashMap (AHash)
// -----------------------------------------------------------------------------
fn bench_dashmap_ahash(keys: &Arc<Vec<String>>) {
    let map = Arc::new(DashMap::<String, u64, RandomState>::with_hasher(RandomState::new()));
    for (i, k) in keys.iter().enumerate() {
        map.insert(k.clone(), i as u64);
    }

    run_benchmark("DashMap (AHash) - 100% Reads", {
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

    run_benchmark("DashMap (AHash) - 90% Read / 10% Write", {
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

    let map_w = Arc::new(DashMap::<String, u64, RandomState>::with_hasher(RandomState::new()));
    run_benchmark("DashMap (AHash) - 100% Inserts", {
        let map = map_w.clone();
        move |tid| {
            let base = tid * OPS_PER_THREAD;
            for i in 0..OPS_PER_THREAD {
                map.insert(format!("ins_da_{}_{}", tid, i), (base + i) as u64);
            }
        }
    });
}

// -----------------------------------------------------------------------------
// 3. scc::HashMap (Default)
// -----------------------------------------------------------------------------
fn bench_scc_default(keys: &Arc<Vec<String>>) {
    let map = Arc::new(SccMap::<String, u64>::default());
    for (i, k) in keys.iter().enumerate() {
        let _ = map.insert_sync(k.clone(), i as u64);
    }

    run_benchmark("scc::HashMap (Default) - 100% Reads", {
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

    run_benchmark("scc::HashMap (Default) - 90% Read / 10% Write", {
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

    let map_w = Arc::new(SccMap::<String, u64>::default());
    run_benchmark("scc::HashMap (Default) - 100% Inserts", {
        let map = map_w.clone();
        move |tid| {
            let base = tid * OPS_PER_THREAD;
            for i in 0..OPS_PER_THREAD {
                let _ = map.insert_sync(format!("ins_s_{}_{}", tid, i), (base + i) as u64);
            }
        }
    });
}

// -----------------------------------------------------------------------------
// 4. scc::HashMap (AHash)
// -----------------------------------------------------------------------------
fn bench_scc_ahash(keys: &Arc<Vec<String>>) {
    let map = Arc::new(SccMap::<String, u64, RandomState>::with_hasher(RandomState::new()));
    for (i, k) in keys.iter().enumerate() {
        let _ = map.insert_sync(k.clone(), i as u64);
    }

    run_benchmark("scc::HashMap (AHash) - 100% Reads", {
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

    run_benchmark("scc::HashMap (AHash) - 90% Read / 10% Write", {
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

    let map_w = Arc::new(SccMap::<String, u64, RandomState>::with_hasher(RandomState::new()));
    run_benchmark("scc::HashMap (AHash) - 100% Inserts", {
        let map = map_w.clone();
        move |tid| {
            let base = tid * OPS_PER_THREAD;
            for i in 0..OPS_PER_THREAD {
                let _ = map.insert_sync(format!("ins_sa_{}_{}", tid, i), (base + i) as u64);
            }
        }
    });
}

// -----------------------------------------------------------------------------
// 5. FyroDB CustomMap (FoldHash + EBR)
// -----------------------------------------------------------------------------
fn bench_fyro_custommap(keys: &Arc<Vec<String>>) {
    // 128 shards, pre-sized capacity for INITIAL_KEYS
    let map = Arc::new(CustomMap::<u64>::with_capacity(128, INITIAL_KEYS * 2));
    for (i, k) in keys.iter().enumerate() {
        map.insert(k.clone(), i as u64);
    }

    // 100% Reads (using wait-free .get())
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

    // 90% Read / 10% Write
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

    // 100% Inserts
    let map_w = Arc::new(CustomMap::<u64>::with_capacity(128, NUM_THREADS * OPS_PER_THREAD));
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

// -----------------------------------------------------------------------------
// Benchmark Runner Helper
// -----------------------------------------------------------------------------
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
