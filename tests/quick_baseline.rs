use std::time::Instant;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use fyro_db::storage::store::Store;
use fyro_db::storage::bitmap::count_ones_fast;
use fyro_db::cluster::SpinMutex;

// Baseline simulation of raw TAS mutex
struct RawTasMutex<T> {
    locked: AtomicBool,
    data: std::cell::UnsafeCell<T>,
}
unsafe impl<T: Send> Sync for RawTasMutex<T> {}
unsafe impl<T: Send> Send for RawTasMutex<T> {}

impl<T> RawTasMutex<T> {
    fn new(data: T) -> Self {
        Self {
            locked: AtomicBool::new(false),
            data: std::cell::UnsafeCell::new(data),
        }
    }
    fn lock(&self) -> RawTasGuard<'_, T> {
        // Raw TAS: repeatedly performs atomic swap RMW on memory bus
        while self.locked.swap(true, Ordering::Acquire) {
            std::hint::spin_loop();
        }
        RawTasGuard { lock: self }
    }
}
struct RawTasGuard<'a, T: 'a> {
    lock: &'a RawTasMutex<T>,
}
impl<T> std::ops::DerefMut for RawTasGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.lock.data.get() }
    }
}
impl<T> std::ops::Deref for RawTasGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { &*self.lock.data.get() }
    }
}
impl<T> Drop for RawTasGuard<'_, T> {
    fn drop(&mut self) {
        self.lock.locked.store(false, Ordering::Release);
    }
}

#[test]
fn test_run_benchmarks_before_and_after() {
    println!("\n========================================================");
    println!("       FYRODB END-TO-END OPTIMIZATION BENCHMARK         ");
    println!("========================================================");

    let store = Arc::new(Store::with_config(8, 200_000));
    let n = 200_000;

    // ---------------------------------------------------------
    // 1. Point SET Throughput
    // ---------------------------------------------------------
    let t0 = Instant::now();
    for i in 0..n {
        let k = format!("key:{:08}", i);
        let v = format!("val:{:08}", i);
        store.set_string(&k, &v, 0);
    }
    let d_set = t0.elapsed();
    let set_mops = (n as f64) / d_set.as_secs_f64() / 1_000_000.0;
    println!("[1] Point SET Throughput:       {:.2} Mops/s ({:?})", set_mops, d_set);

    // ---------------------------------------------------------
    // 2. Point GET Throughput
    // ---------------------------------------------------------
    let t0 = Instant::now();
    let mut hits = 0;
    for i in 0..n {
        let k = format!("key:{:08}", i);
        if store.get(&k).is_some() {
            hits += 1;
        }
    }
    let d_get = t0.elapsed();
    let get_mops = (n as f64) / d_get.as_secs_f64() / 1_000_000.0;
    println!("[2] Point GET Throughput:       {:.2} Mops/s ({:?}, hits={})", get_mops, d_get, hits);

    // ---------------------------------------------------------
    // 3. Multi-Threaded Contention: Raw TAS vs True TTAS (SpinMutex)
    // ---------------------------------------------------------
    let threads = 8;
    let ops_per_thread = 50_000;

    // A) Raw TAS under 8-thread contention
    let raw_mutex = Arc::new(RawTasMutex::new(0u64));
    let t0 = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..threads {
        let m = Arc::clone(&raw_mutex);
        handles.push(std::thread::spawn(move || {
            for _ in 0..ops_per_thread {
                let mut g = m.lock();
                *g += 1;
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let d_tas = t0.elapsed();
    let tas_mops = ((threads * ops_per_thread) as f64) / d_tas.as_secs_f64() / 1_000_000.0;

    // B) True TTAS (SpinMutex) under 8-thread contention
    let ttas_mutex = Arc::new(SpinMutex::new(0u64));
    let t0 = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..threads {
        let m = Arc::clone(&ttas_mutex);
        handles.push(std::thread::spawn(move || {
            for _ in 0..ops_per_thread {
                let mut g = m.lock();
                *g += 1;
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let d_ttas = t0.elapsed();
    let ttas_mops = ((threads * ops_per_thread) as f64) / d_ttas.as_secs_f64() / 1_000_000.0;
    let lock_gain = (d_tas.as_secs_f64() / d_ttas.as_secs_f64() - 1.0) * 100.0;

    println!("[3] Lock Contention (8-Thread):");
    println!("    - Raw TAS Mutex (BEFORE):   {:.2} Mops/s ({:?})", tas_mops, d_tas);
    println!("    - True TTAS Mutex (AFTER):  {:.2} Mops/s ({:?}) [Gain: +{:.1}%]", ttas_mops, d_ttas, lock_gain);

    // ---------------------------------------------------------
    // 4. HLL: Hardware TZCNT + Reciprocal Table (PFADD & PFCOUNT)
    // ---------------------------------------------------------
    let mut hll = fyro_db::storage::hll::HllData::new();
    let t0 = Instant::now();
    for i in 0..100_000 {
        let item = format!("item:{:08}", i);
        hll.add(item.as_bytes());
    }
    let d_hll_add = t0.elapsed();
    let hll_add_mops = 100_000.0 / d_hll_add.as_secs_f64() / 1_000_000.0;

    // Measure AFTER PFCOUNT with static reciprocal table
    let t0 = Instant::now();
    let mut count = 0;
    for _ in 0..10_000 {
        count = std::hint::black_box(hll.count());
    }
    let d_hll_count_after = t0.elapsed();

    // Baseline PFCOUNT (simulating unoptimized 16,384 2^-R FDIV loop)
    let t0 = Instant::now();
    let mut count_before = 0.0f64;
    for _ in 0..10_000 {
        let mut sum = 0.0f64;
        for &reg in &hll.registers {
            sum += 1.0 / (1u64 << reg) as f64;
        }
        count_before += std::hint::black_box(sum);
    }
    let d_hll_count_before = t0.elapsed();
    let hll_speedup = d_hll_count_before.as_secs_f64() / d_hll_count_after.as_secs_f64();

    println!("[4] HyperLogLog (100k items):");
    println!("    - PFADD (HW TZCNT):         {:.2} Mops/s ({:?})", hll_add_mops, d_hll_add);
    println!("    - 10k PFCOUNT (BEFORE FDIV):{:?} (sum={:.1})", d_hll_count_before, count_before);
    println!("    - 10k PFCOUNT (AFTER Table):{:?} [{:.2}x faster, count={}]", d_hll_count_after, hll_speedup, count);

    // ---------------------------------------------------------
    // 5. Bitmap: 1MB BITCOUNT (Scalar byte loop vs 64-bit POPCNT)
    // ---------------------------------------------------------
    let bitmap_bytes = vec![0x55u8; 1024 * 1024]; // 1 MB bitmap
    let iters = 100;

    // A) Scalar byte loop (BEFORE)
    let t0 = Instant::now();
    let mut bits_before = 0u64;
    for _ in 0..iters {
        let mut bits = 0u64;
        for &b in &bitmap_bytes {
            bits += b.count_ones() as u64;
        }
        bits_before += std::hint::black_box(bits);
    }
    let d_bit_before = t0.elapsed();

    // B) Fast word-aligned POPCNT (AFTER)
    let t0 = Instant::now();
    let mut bits_after = 0usize;
    for _ in 0..iters {
        bits_after += std::hint::black_box(count_ones_fast(&bitmap_bytes));
    }
    let d_bit_after = t0.elapsed();
    let bit_speedup = d_bit_before.as_secs_f64() / d_bit_after.as_secs_f64();

    println!("[5] Bitmap 1MB BITCOUNT (x100 runs):");
    println!("    - Scalar Byte Loop (BEFORE):{:?} (bits={})", d_bit_before, bits_before);
    println!("    - 64-bit POPCNT Fast (AFTER):{:?} [{:.2}x faster, bits={}]", d_bit_after, bit_speedup, bits_after);

    // ---------------------------------------------------------
    // 6. ZSet: Upper-Half Rank Search (Bidirectional Chunks)
    // ---------------------------------------------------------
    store.zadd("zkey", &[
        (10.0, "m1"), (20.0, "m2"), (30.0, "m3"), (40.0, "m4"),
        (50.0, "m5"), (60.0, "m6"), (70.0, "m7"), (80.0, "m8"),
        (90.0, "m9"), (100.0, "m10"),
    ], fyro_db::storage::zset::ZAddOptions::default()).unwrap();

    let t0 = Instant::now();
    for _ in 0..100_000 {
        let _ = store.zrank("zkey", "m9");
    }
    let d_zrank = t0.elapsed();
    println!("[6] ZSet Upper-Half Rank (100k ops): {:?}", d_zrank);

    println!("========================================================\n");
}
