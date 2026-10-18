use std::sync::Arc;
use std::time::Instant;
use customhash::CustomMap;

const INITIAL_KEYS: usize = 50_000;
const OPS_PER_THREAD: usize = 500_000;

fn main() {
    println!("\n╔═════════════════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║         🔥 FYRODB CUSTOMMAP (customhash) - DETAILED MULTI-CORE BREAKDOWN BENCHMARK 🔥         ║");
    println!("╚═════════════════════════════════════════════════════════════════════════════════════════════════╝");
    println!("  Initial Keys: {} | Operations per Thread: {}\n", INITIAL_KEYS, OPS_PER_THREAD);

    let keys: Arc<Vec<String>> = Arc::new(
        (0..INITIAL_KEYS)
            .map(|i| format!("k:{:010}", i))
            .collect()
    );

    for &threads in &[1, 2, 4, 6, 8, 12, 16, 32] {
        println!("┌─────────────────────────────────────────────────────────────────────────────────────────┐");
        println!("│  THREAD COUNT: {:2} WORKERS / THREADS                                                   │", threads);
        println!("├─────────────────────────────────────────────────────────────────────────────────────────┤");

        let map = Arc::new(CustomMap::<u64>::with_capacity(128, INITIAL_KEYS * 2));
        for (i, k) in keys.iter().enumerate() {
            map.insert(k.clone(), i as u64);
        }

        // 1. GET (100% Reads)
        bench_workload("GET (100% Read Hits)", threads, {
            let map = map.clone();
            let keys = keys.clone();
            move |tid| {
                let start = tid * (OPS_PER_THREAD / 2);
                let mut sum = 0u64;
                for i in 0..OPS_PER_THREAD {
                    let k = &keys[(start + i) % INITIAL_KEYS];
                    if let Some(v) = map.get(k.as_str()) {
                        sum = sum.wrapping_add(v);
                    }
                }
                std::hint::black_box(sum);
            }
        });

        // 2. SET Overwrite (Update existing keys in-place)
        bench_workload("SET (Overwrite In-Place)", threads, {
            let map = map.clone();
            let keys = keys.clone();
            move |tid| {
                let start = tid * (OPS_PER_THREAD / 2);
                for i in 0..OPS_PER_THREAD {
                    let k = &keys[(start + i) % INITIAL_KEYS];
                    map.set(k.as_str(), (start + i) as u64, || k.clone());
                }
            }
        });

        // 3. Mixed 90% Read / 10% Write
        bench_workload("Mixed (90% Read / 10% Write)", threads, {
            let map = map.clone();
            let keys = keys.clone();
            move |tid| {
                let start = tid * (OPS_PER_THREAD / 2);
                for i in 0..OPS_PER_THREAD {
                    let k = &keys[(start + i) % INITIAL_KEYS];
                    if i % 10 == 0 {
                        map.set(k.as_str(), (start + i) as u64, || k.clone());
                    } else {
                        let _ = map.get(k.as_str());
                    }
                }
            }
        });

        // 4. GET Miss (Non-existent keys)
        let missing_keys: Arc<Vec<String>> = Arc::new(
            (0..INITIAL_KEYS)
                .map(|i| format!("missing:{:010}", i))
                .collect()
        );
        bench_workload("GET Miss (Non-existent)", threads, {
            let map = map.clone();
            let keys = missing_keys.clone();
            move |tid| {
                let start = tid * (OPS_PER_THREAD / 2);
                for i in 0..OPS_PER_THREAD {
                    let k = &keys[(start + i) % INITIAL_KEYS];
                    let _ = map.get(k.as_str());
                }
            }
        });

        println!("└─────────────────────────────────────────────────────────────────────────────────────────┘\n");
    }
}

fn bench_workload<F>(name: &str, threads: usize, task_factory: F)
where
    F: Fn(usize) + Send + Sync + 'static + Clone,
{
    let total_ops = threads * OPS_PER_THREAD;
    let mut handles = Vec::with_capacity(threads);

    let start = Instant::now();
    for thread_id in 0..threads {
        let task = task_factory.clone();
        handles.push(std::thread::spawn(move || {
            task(thread_id);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
    let elapsed = start.elapsed();
    let mops = (total_ops as f64 / 1_000_000.0) / elapsed.as_secs_f64();
    let ns_per_op = (elapsed.as_nanos() as f64) / (total_ops as f64);

    println!(
        "  │ {:<32} │ {:>7.2?} │ {:>8.2} M ops/sec │ {:>6.2} ns/op │",
        name, elapsed, mops, ns_per_op
    );
}
