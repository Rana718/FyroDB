use std::sync::Arc;
use std::time::Instant;
use ahash::RandomState;
use customhash::CustomMap;
use dashmap::DashMap;
use scc::HashMap as SccMap;

const NUM_THREADS: usize = 8;
const OPS_PER_THREAD: usize = 100_000;
const INITIAL_KEYS: usize = 20_000;

#[test]
fn test_dashmap_vs_scc_vs_fyrodb_custommap() {
    println!("\n=========================================================================================");
    println!("     CONCURRENT HASHMAP TEST: DashMap vs scc::HashMap vs FyroDB CustomMap");
    println!("     Workload: String Keys | Threads: {} | Keys: {} | Ops/Thread: {}", 
             NUM_THREADS, INITIAL_KEYS, OPS_PER_THREAD);
    println!("=========================================================================================");

    let keys: Arc<Vec<String>> = Arc::new(
        (0..INITIAL_KEYS)
            .map(|i| format!("sess_key_{:08}", i))
            .collect()
    );

    // 1. DashMap Default
    let (d_def_r, d_def_mix, d_def_w) = bench_dashmap_default(&keys);

    // 2. DashMap AHash
    let (d_ah_r, d_ah_mix, d_ah_w) = bench_dashmap_ahash(&keys);

    // 3. scc::HashMap Default
    let (s_def_r, s_def_mix, s_def_w) = bench_scc_default(&keys);

    // 4. scc::HashMap AHash
    let (s_ah_r, s_ah_mix, s_ah_w) = bench_scc_ahash(&keys);

    // 5. FyroDB CustomMap
    let (f_r, f_mix, f_w) = bench_fyro_custommap(&keys);

    println!("\n=========================================================================================");
    println!("                                   FINAL LEADERBOARD");
    println!("=========================================================================================");
    println!("{:<35} | {:<14} | {:<14} | {:<14}", "Implementation", "100% Read", "90% R / 10% W", "100% Insert");
    println!("-----------------------------------------------------------------------------------------");
    println!("{:<35} | {:>10.2} M/s | {:>10.2} M/s | {:>10.2} M/s", "DashMap (Default SipHash)", d_def_r, d_def_mix, d_def_w);
    println!("{:<35} | {:>10.2} M/s | {:>10.2} M/s | {:>10.2} M/s", "DashMap (AHash)", d_ah_r, d_ah_mix, d_ah_w);
    println!("{:<35} | {:>10.2} M/s | {:>10.2} M/s | {:>10.2} M/s", "scc::HashMap (Default)", s_def_r, s_def_mix, s_def_w);
    println!("{:<35} | {:>10.2} M/s | {:>10.2} M/s | {:>10.2} M/s", "scc::HashMap (AHash)", s_ah_r, s_ah_mix, s_ah_w);
    println!("{:<35} | {:>10.2} M/s | {:>10.2} M/s | {:>10.2} M/s", "FyroDB CustomMap (FoldHash+EBR)", f_r, f_mix, f_w);
    println!("=========================================================================================\n");
}

fn bench_dashmap_default(keys: &Arc<Vec<String>>) -> (f64, f64, f64) {
    let map = Arc::new(DashMap::<String, u64>::new());
    for (i, k) in keys.iter().enumerate() {
        map.insert(k.clone(), i as u64);
    }
    let r = run_bench({
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

    let mix = run_bench({
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

    let map_w = Arc::new(DashMap::<String, u64>::new());
    let w = run_bench({
        let map = map_w.clone();
        move |tid| {
            let base = tid * OPS_PER_THREAD;
            for i in 0..OPS_PER_THREAD {
                map.insert(format!("ins_d_{}_{}", tid, i), (base + i) as u64);
            }
        }
    });
    (r, mix, w)
}

fn bench_dashmap_ahash(keys: &Arc<Vec<String>>) -> (f64, f64, f64) {
    let map = Arc::new(DashMap::<String, u64, RandomState>::with_hasher(RandomState::new()));
    for (i, k) in keys.iter().enumerate() {
        map.insert(k.clone(), i as u64);
    }
    let r = run_bench({
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

    let mix = run_bench({
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
    let w = run_bench({
        let map = map_w.clone();
        move |tid| {
            let base = tid * OPS_PER_THREAD;
            for i in 0..OPS_PER_THREAD {
                map.insert(format!("ins_da_{}_{}", tid, i), (base + i) as u64);
            }
        }
    });
    (r, mix, w)
}

fn bench_scc_default(keys: &Arc<Vec<String>>) -> (f64, f64, f64) {
    let map = Arc::new(SccMap::<String, u64>::default());
    for (i, k) in keys.iter().enumerate() {
        let _ = map.insert_sync(k.clone(), i as u64);
    }
    let r = run_bench({
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

    let mix = run_bench({
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
    let w = run_bench({
        let map = map_w.clone();
        move |tid| {
            let base = tid * OPS_PER_THREAD;
            for i in 0..OPS_PER_THREAD {
                let _ = map.insert_sync(format!("ins_s_{}_{}", tid, i), (base + i) as u64);
            }
        }
    });
    (r, mix, w)
}

fn bench_scc_ahash(keys: &Arc<Vec<String>>) -> (f64, f64, f64) {
    let map = Arc::new(SccMap::<String, u64, RandomState>::with_hasher(RandomState::new()));
    for (i, k) in keys.iter().enumerate() {
        let _ = map.insert_sync(k.clone(), i as u64);
    }
    let r = run_bench({
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

    let mix = run_bench({
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
    let w = run_bench({
        let map = map_w.clone();
        move |tid| {
            let base = tid * OPS_PER_THREAD;
            for i in 0..OPS_PER_THREAD {
                let _ = map.insert_sync(format!("ins_sa_{}_{}", tid, i), (base + i) as u64);
            }
        }
    });
    (r, mix, w)
}

fn bench_fyro_custommap(keys: &Arc<Vec<String>>) -> (f64, f64, f64) {
    let map = Arc::new(CustomMap::<u64>::with_capacity(64, INITIAL_KEYS * 2));
    for (i, k) in keys.iter().enumerate() {
        map.insert(k.clone(), i as u64);
    }
    let r = run_bench({
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

    let mix = run_bench({
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

    let map_w = Arc::new(CustomMap::<u64>::with_capacity(64, NUM_THREADS * OPS_PER_THREAD));
    let w = run_bench({
        let map = map_w.clone();
        move |tid| {
            let base = tid * OPS_PER_THREAD;
            for i in 0..OPS_PER_THREAD {
                map.insert(format!("ins_fy_{}_{}", tid, i), (base + i) as u64);
            }
        }
    });
    (r, mix, w)
}

fn run_bench<F>(f: F) -> f64
where
    F: Fn(usize) + Send + Sync + 'static + Clone,
{
    let total_ops = NUM_THREADS * OPS_PER_THREAD;
    let mut handles = Vec::with_capacity(NUM_THREADS);
    let start = Instant::now();
    for tid in 0..NUM_THREADS {
        let task = f.clone();
        handles.push(std::thread::spawn(move || task(tid)));
    }
    for h in handles {
        h.join().unwrap();
    }
    let elapsed = start.elapsed().as_secs_f64();
    (total_ops as f64 / 1_000_000.0) / elapsed
}
