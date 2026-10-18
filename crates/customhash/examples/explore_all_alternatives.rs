use std::sync::Arc;
use std::time::Instant;
use foldhash::fast::RandomState as FoldRandomState;
use customhash::CustomMap;
use scc::HashMap as SccMap;

const NUM_THREADS: usize = 16;
const OPS_PER_THREAD: usize = 200_000;
const INITIAL_KEYS: usize = 50_000;

fn main() {
    println!("=========================================================================================");
    println!("        BEYOND SCC: EXPLORING ALL HIGH-PERFORMANCE CONCURRENT OPTIONS");
    println!("  Threads: {} | Initial Keys: {} | Ops/Thread: {} | Total Ops: 3.2M", 
             NUM_THREADS, INITIAL_KEYS, OPS_PER_THREAD);
    println!("=========================================================================================\n");

    let keys: Arc<Vec<String>> = Arc::new(
        (0..INITIAL_KEYS)
            .map(|i| format!("usr_sess_{:010}", i))
            .collect()
    );

    // 1. scc::HashMap + FoldHash
    println!(">>> 1. scc::HashMap (FoldHash) <<<");
    bench_scc(&keys);

    // 2. Papaya (FoldHash) - Famous Lock-Free Hash Table
    println!("\n>>> 2. Papaya (Lock-Free Map with FoldHash) <<<");
    bench_papaya(&keys);

    // 3. Flurry (Java ConcurrentHashMap Port)
    println!("\n>>> 3. Flurry (ConcurrentHashMap Port) <<<");
    bench_flurry(&keys);

    // 4. FyroDB CustomMap (FoldHash + EBR)
    println!("\n>>> 4. FyroDB CustomMap (FoldHash + EBR) <<<");
    bench_fyro_custommap(&keys);

    // 5. Game Architecture Alternative: Direct Flat Slot Array (Zero-Hashing)
    println!("\n>>> 5. Game Architecture: Direct Slot Index Array (No Hashing Overhead) <<<");
    bench_direct_slot_array();

    println!("\n=========================================================================================");
    println!("                               EXPERIMENT FINISHED");
    println!("=========================================================================================");
}

fn bench_scc(keys: &Arc<Vec<String>>) {
    let map = Arc::new(SccMap::<String, u64, FoldRandomState>::with_hasher(FoldRandomState::default()));
    for (i, k) in keys.iter().enumerate() {
        let _ = map.insert_sync(k.clone(), i as u64);
    }

    run_benchmark("scc (FoldHash) - 100% Reads", {
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

    run_benchmark("scc (FoldHash) - 90% Read / 10% Write", {
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

    let map_w = Arc::new(SccMap::<String, u64, FoldRandomState>::with_hasher(FoldRandomState::default()));
    run_benchmark("scc (FoldHash) - 100% Inserts", {
        let map = map_w.clone();
        move |tid| {
            let base = tid * OPS_PER_THREAD;
            for i in 0..OPS_PER_THREAD {
                let _ = map.insert_sync(format!("ins_s_{}_{}", tid, i), (base + i) as u64);
            }
        }
    });
}

fn bench_papaya(keys: &Arc<Vec<String>>) {
    let map = Arc::new(
        papaya::HashMap::<String, u64, FoldRandomState>::with_hasher(FoldRandomState::default())
    );
    let pin = map.pin();
    for (i, k) in keys.iter().enumerate() {
        pin.insert(k.clone(), i as u64);
    }

    run_benchmark("Papaya (FoldHash) - 100% Reads", {
        let map = map.clone();
        let keys = keys.clone();
        move |tid| {
            let pin = map.pin();
            let start = tid * (OPS_PER_THREAD / 2);
            let mut sum = 0u64;
            for i in 0..OPS_PER_THREAD {
                let key = &keys[(start + i) % INITIAL_KEYS];
                if let Some(v) = pin.get(key.as_str()) {
                    sum = sum.wrapping_add(*v);
                }
            }
            std::hint::black_box(sum);
        }
    });

    run_benchmark("Papaya (FoldHash) - 90% Read / 10% Write", {
        let map = map.clone();
        let keys = keys.clone();
        move |tid| {
            let pin = map.pin();
            let start = tid * (OPS_PER_THREAD / 2);
            for i in 0..OPS_PER_THREAD {
                let key = &keys[(start + i) % INITIAL_KEYS];
                if i % 10 == 0 {
                    pin.insert(key.clone(), i as u64);
                } else {
                    let _ = pin.get(key.as_str());
                }
            }
        }
    });

    let map_w = Arc::new(
        papaya::HashMap::<String, u64, FoldRandomState>::with_hasher(FoldRandomState::default())
    );
    run_benchmark("Papaya (FoldHash) - 100% Inserts", {
        let map = map_w.clone();
        move |tid| {
            let pin = map.pin();
            let base = tid * OPS_PER_THREAD;
            for i in 0..OPS_PER_THREAD {
                pin.insert(format!("ins_p_{}_{}", tid, i), (base + i) as u64);
            }
        }
    });
}

fn bench_flurry(keys: &Arc<Vec<String>>) {
    let map = Arc::new(flurry::HashMap::<String, u64>::new());
    let pin = map.pin();
    for (i, k) in keys.iter().enumerate() {
        pin.insert(k.clone(), i as u64);
    }

    run_benchmark("Flurry - 100% Reads", {
        let map = map.clone();
        let keys = keys.clone();
        move |tid| {
            let pin = map.pin();
            let start = tid * (OPS_PER_THREAD / 2);
            let mut sum = 0u64;
            for i in 0..OPS_PER_THREAD {
                let key = &keys[(start + i) % INITIAL_KEYS];
                if let Some(v) = pin.get(key.as_str()) {
                    sum = sum.wrapping_add(*v);
                }
            }
            std::hint::black_box(sum);
        }
    });

    run_benchmark("Flurry - 90% Read / 10% Write", {
        let map = map.clone();
        let keys = keys.clone();
        move |tid| {
            let pin = map.pin();
            let start = tid * (OPS_PER_THREAD / 2);
            for i in 0..OPS_PER_THREAD {
                let key = &keys[(start + i) % INITIAL_KEYS];
                if i % 10 == 0 {
                    pin.insert(key.clone(), i as u64);
                } else {
                    let _ = pin.get(key.as_str());
                }
            }
        }
    });

    let map_w = Arc::new(flurry::HashMap::<String, u64>::new());
    run_benchmark("Flurry - 100% Inserts", {
        let map = map_w.clone();
        move |tid| {
            let pin = map.pin();
            let base = tid * OPS_PER_THREAD;
            for i in 0..OPS_PER_THREAD {
                pin.insert(format!("ins_f_{}_{}", tid, i), (base + i) as u64);
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

    let map_w = Arc::new(CustomMap::<u64>::with_capacity(64, NUM_THREADS * OPS_PER_THREAD));
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

// In AAA game engines, SessionID or RoomID is an integer (index into a flat array).
// No hash calculation, no bucket searching, direct atomic/unfenced memory read.
fn bench_direct_slot_array() {
    use std::sync::atomic::{AtomicU64, Ordering};
    let array: Arc<Vec<AtomicU64>> = Arc::new(
        (0..INITIAL_KEYS as u64).map(|i| AtomicU64::new(i * 2)).collect()
    );

    run_benchmark("Direct Slot Array - 100% Reads", {
        let array = array.clone();
        move |tid| {
            let start = tid * (OPS_PER_THREAD / 2);
            let mut sum = 0u64;
            for i in 0..OPS_PER_THREAD {
                let idx = (start + i) % INITIAL_KEYS;
                sum = sum.wrapping_add(array[idx].load(Ordering::Relaxed));
            }
            std::hint::black_box(sum);
        }
    });

    run_benchmark("Direct Slot Array - 90% Read / 10% Write", {
        let array = array.clone();
        move |tid| {
            let start = tid * (OPS_PER_THREAD / 2);
            for i in 0..OPS_PER_THREAD {
                let idx = (start + i) % INITIAL_KEYS;
                if i % 10 == 0 {
                    array[idx].store(i as u64, Ordering::Relaxed);
                } else {
                    let _ = array[idx].load(Ordering::Relaxed);
                }
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
