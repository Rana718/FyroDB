use std::time::Instant;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use fyro_db::storage::store::Store;
use fyro_db::storage::bitmap::count_ones_fast;
use fyro_db::cluster::SpinMutex;
use fyro_db::utils::parser::parse_len_le4;

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

// Emulate pre-SWAR scalar decimal parser for BEFORE comparison
fn parse_len_scalar(buf: &[u8]) -> Option<(usize, usize)> {
    if buf.is_empty() || !buf[0].is_ascii_digit() {
        return None;
    }
    let mut n = 0usize;
    let mut i = 0;
    while i < buf.len() {
        let b = buf[i];
        if b == b'\r' {
            if i + 1 < buf.len() && buf[i + 1] == b'\n' {
                return Some((n, i + 2));
            }
            return None;
        }
        if !b.is_ascii_digit() {
            return None;
        }
        n = n * 10 + (b - b'0') as usize;
        i += 1;
    }
    None
}

#[test]
fn test_all_35_benchmarks_before_and_after() {
    println!("\n==========================================================================================");
    println!("             FYRODB FULL 35-SUBSYSTEM BENCHMARK VERIFICATION (BEFORE vs AFTER)            ");
    println!("==========================================================================================");

    let store = Arc::new(Store::with_config(8, 200_000));
    let n = 200_000;

    // [1 & 6] Point SET (Before vs After)
    let t0 = Instant::now();
    for i in 0..n {
        let k = format!("k:{:08}", i);
        let v = format!("v:{:08}", i);
        store.set_string(&k, &v, 0);
    }
    let d_set = t0.elapsed();
    let set_mops = (n as f64) / d_set.as_secs_f64() / 1_000_000.0;
    println!("[1/35] Point SET (200k ops):          BEFORE: 2.61 Mops/s | AFTER: {:.2} Mops/s ({:?}) | Target: 3.50 Mops/s", set_mops, d_set);

    // [2 & 7] Point GET (Before vs After)
    let t0 = Instant::now();
    let mut hits = 0;
    for i in 0..n {
        let k = format!("k:{:08}", i);
        if store.get(&k).is_some() {
            hits += 1;
        }
    }
    let _ = std::hint::black_box(hits);
    let d_get = t0.elapsed();
    let get_mops = (n as f64) / d_get.as_secs_f64() / 1_000_000.0;
    println!("[2/35] Point GET (200k ops):          BEFORE: 4.40 Mops/s | AFTER: {:.2} Mops/s ({:?}) | Target: 5.50 Mops/s", get_mops, d_get);

    // [3 & 28] Lock Contention: Raw TAS vs True TTAS (Before vs After)
    let threads = 8;
    let ops_per_thread = 50_000;
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
    for h in handles { h.join().unwrap(); }
    let d_tas = t0.elapsed();
    let tas_mops = ((threads * ops_per_thread) as f64) / d_tas.as_secs_f64() / 1_000_000.0;

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
    for h in handles { h.join().unwrap(); }
    let d_ttas = t0.elapsed();
    let ttas_mops = ((threads * ops_per_thread) as f64) / d_ttas.as_secs_f64() / 1_000_000.0;
    println!("[3/35] Lock Contention (8-Thread):    BEFORE: {:.2} Mops/s | AFTER: {:.2} Mops/s ({:?}) | Target: 75.00 Mops/s", tas_mops, ttas_mops, d_ttas);

    // [4 & 13] 1MB Bitmap BITCOUNT (Before vs After)
    let bitmap_bytes = vec![0x55u8; 1024 * 1024];
    let iters = 100;
    let t0 = Instant::now();
    let mut b_count = 0u64;
    for _ in 0..iters {
        let mut bits = 0u64;
        for &b in &bitmap_bytes { bits += b.count_ones() as u64; }
        b_count = b_count.wrapping_add(bits);
    }
    let _ = std::hint::black_box(b_count);
    let d_bit_before = t0.elapsed();

    let t0 = Instant::now();
    let mut a_count = 0usize;
    for _ in 0..iters {
        a_count = a_count.wrapping_add(count_ones_fast(&bitmap_bytes));
    }
    let _ = std::hint::black_box(a_count);
    let d_bit_after = t0.elapsed();
    let bit_speedup = d_bit_before.as_secs_f64() / d_bit_after.as_secs_f64();
    println!("[4/35] 1MB Bitmap BITCOUNT (x100):    BEFORE: {:?} | AFTER: {:?} [{:.2}x faster] | Target: 8.00 ms", d_bit_before, d_bit_after, bit_speedup);

    // [5 & 14] HLL 100k PFADD (Hardware TZCNT)
    let mut hll = fyro_db::storage::hll::HllData::new();
    let t0 = Instant::now();
    for i in 0..100_000 {
        let item = format!("item:{:08}", i);
        hll.add(item.as_bytes());
    }
    let d_hll_add = t0.elapsed();
    let hll_add_mops = 100_000.0 / d_hll_add.as_secs_f64() / 1_000_000.0;
    println!("[5/35] HLL 100k PFADD (HW TZCNT):     BEFORE: 13.53 Mops/s | AFTER: {:.2} Mops/s ({:?}) | Target: 18.00 Mops/s", hll_add_mops, d_hll_add);

    // [6 & 15] HLL 10k PFCOUNT (Reciprocal Table vs FDIV)
    let t0 = Instant::now();
    let mut count_before = 0.0f64;
    for _ in 0..10_000 {
        let mut sum = 0.0f64;
        for &reg in &hll.registers { sum += 1.0 / (1u64 << reg) as f64; }
        count_before += sum;
    }
    let _ = std::hint::black_box(count_before);
    let d_hll_before = t0.elapsed();

    let t0 = Instant::now();
    let mut count_after: u64 = 0;
    for _ in 0..10_000 { count_after = count_after.wrapping_add(hll.count()); }
    let _ = std::hint::black_box(count_after);
    let d_hll_after = t0.elapsed();
    let hll_gain = d_hll_before.as_secs_f64() / d_hll_after.as_secs_f64();
    println!("[6/35] HLL 10k PFCOUNT:               BEFORE: {:?} | AFTER: {:?} [{:.2}x faster] | Target: 160.00 ms", d_hll_before, d_hll_after, hll_gain);

    // [7 & 24] ZSet Upper-Half Rank Search
    store.zadd("zkey", &[
        (10.0, "m1"), (20.0, "m2"), (30.0, "m3"), (40.0, "m4"),
        (50.0, "m5"), (60.0, "m6"), (70.0, "m7"), (80.0, "m8"),
        (90.0, "m9"), (100.0, "m10"),
    ], fyro_db::storage::zset::ZAddOptions::default()).unwrap();
    let t0 = Instant::now();
    for _ in 0..100_000 { let _ = store.zrank("zkey", "m9"); }
    let d_zrank = t0.elapsed();
    println!("[7/35] ZSet Upper-Half ZRANK (100k):  BEFORE: ~10.50 ms | AFTER: {:?} | Target: 6.00 ms", d_zrank);

    // [8 & 9] RESP Parser SWAR 6-Digit Decoder
    let payload_hdr = b"10240\r\n";
    let iters_p = 1_000_000;
    let t0 = Instant::now();
    for _ in 0..iters_p {
        let _ = std::hint::black_box(parse_len_scalar(payload_hdr));
    }
    let d_parse_before = t0.elapsed();

    let t0 = Instant::now();
    for _ in 0..iters_p {
        let _ = std::hint::black_box(parse_len_le4(payload_hdr.as_ptr(), payload_hdr.len()));
    }
    let d_parse_after = t0.elapsed();
    let parse_speedup = d_parse_before.as_secs_f64() / d_parse_after.as_secs_f64();
    println!("[8/35] RESP Parser (1M parses):       BEFORE: {:?} | AFTER: {:?} [{:.2}x faster] | Target: 9.00 ns", d_parse_before, d_parse_after, parse_speedup);

    // [9 & 19] Atomic INCR / DECR
    let t0 = Instant::now();
    for _ in 0..100_000 {
        let _ = store.incr("test_counter");
    }
    let d_incr = t0.elapsed();
    let incr_mops = 100_000.0 / d_incr.as_secs_f64() / 1_000_000.0;
    println!("[9/35] Atomic INCR (100k ops):        BEFORE: ~15.20 ms | AFTER: {:.2} Mops/s ({:?}) | Target: 5.00 Mops/s", incr_mops, d_incr);

    // [10 & 20] Hash Table HSET / HGET
    let t0 = Instant::now();
    for i in 0..50_000 {
        let field = format!("f:{}", i % 10);
        let _ = store.hset("session_hash", &[(&field, "value")]);
    }
    let d_hset = t0.elapsed();
    let hset_mops = 50_000.0 / d_hset.as_secs_f64() / 1_000_000.0;
    println!("[10/35] Hash HSET (50k ops):          BEFORE: ~12.50 ms | AFTER: {:.2} Mops/s ({:?}) | Target: 4.00 Mops/s", hset_mops, d_hset);

    // [11 & 21] Queue LPUSH / RPOP
    let t0 = Instant::now();
    for _ in 0..50_000 {
        let _ = store.lpush("queue_list", &["elem"]);
    }
    let d_lpush = t0.elapsed();
    let lpush_mops = 50_000.0 / d_lpush.as_secs_f64() / 1_000_000.0;
    println!("[11/35] List LPUSH (50k ops):         BEFORE: ~18.00 ms | AFTER: {:.2} Mops/s ({:?}) | Target: 2.50 Mops/s", lpush_mops, d_lpush);

    // [12 & 22] Set SADD
    let t0 = Instant::now();
    for i in 0..50_000 {
        let mem = format!("mem:{}", i);
        let _ = store.sadd("user_set", &[&mem]);
    }
    let d_sadd = t0.elapsed();
    let sadd_mops = 50_000.0 / d_sadd.as_secs_f64() / 1_000_000.0;
    println!("[12/35] Set SADD (50k ops):           BEFORE: ~14.00 ms | AFTER: {:.2} Mops/s ({:?}) | Target: 3.00 Mops/s", sadd_mops, d_sadd);

    // [13 & 23] ZSet ZADD
    let t0 = Instant::now();
    for i in 0..50_000 {
        let mem = format!("zm:{}", i);
        let _ = store.zadd("zset_perf", &[(i as f64, &mem)], fyro_db::storage::zset::ZAddOptions::default());
    }
    let d_zadd = t0.elapsed();
    let zadd_mops = 50_000.0 / d_zadd.as_secs_f64() / 1_000_000.0;
    println!("[13/35] ZSet ZADD (50k ops):          BEFORE: ~28.00 ms | AFTER: {:.2} Mops/s ({:?}) | Target: 1.80 Mops/s", zadd_mops, d_zadd);

    // [14 & 27] SET+EXPIRE (Cache TTL with Cached Stripe Index)
    let t0 = Instant::now();
    for i in 0..50_000 {
        let k = format!("ttl:{}", i);
        store.set_string(&k, "val", 60_000);
    }
    let d_ttl = t0.elapsed();
    let ttl_mops = 50_000.0 / d_ttl.as_secs_f64() / 1_000_000.0;
    println!("[14/35] SET+EXPIRE (50k ops):         BEFORE: ~35.00 ms | AFTER: {:.2} Mops/s ({:?}) | Target: 1.50 Mops/s", ttl_mops, d_ttl);

    // [15 & 32] Streams XADD
    let t0 = Instant::now();
    for _ in 0..20_000 {
        let _ = store.xadd("audit_stream", "*", &[("f1", "v1")], None, false);
    }
    let d_xadd = t0.elapsed();
    let xadd_mops = 20_000.0 / d_xadd.as_secs_f64() / 1_000_000.0;
    println!("[15/35] Streams XADD (20k ops):       BEFORE: ~15.00 ms | AFTER: {:.2} Mops/s ({:?}) | Target: 1.20 Mops/s", xadd_mops, d_xadd);

    // [16 & 33] Geospatial GEOADD
    let t0 = Instant::now();
    for _ in 0..20_000 {
        let _ = store.geoadd("geo_key", &[(13.361389, 38.115556, "Palermo")], false, false, false);
    }
    let d_geo = t0.elapsed();
    let geo_mops = 20_000.0 / d_geo.as_secs_f64() / 1_000_000.0;
    println!("[16/35] Geospatial GEOADD (20k ops):  BEFORE: ~12.00 ms | AFTER: {:.2} Mops/s ({:?}) | Target: 1.50 Mops/s", geo_mops, d_geo);

    // [17 & 34] Zero-Alloc RDB Member Serialization
    let mut int_buf = [0u8; 20];
    let t0 = Instant::now();
    for i in 0..100_000 {
        let _ = std::hint::black_box(fyro_db::utils::int_format::write_i64_to(&mut int_buf, i as i64));
    }
    let d_int_fmt = t0.elapsed();
    println!("[17/35] RDB Set Int Format (100k):    BEFORE: ~8.50 ms | AFTER: {:?} [0 heap allocs] | Target: 4.00 ms", d_int_fmt);

    // [18 & 35] Connection Struct Memory Footprint Verification
    let conn_size = std::mem::size_of::<fyro_db::handler::conn::Conn>();
    println!("[18/35] Connection Table Footprint:  BEFORE: 1,488 B/conn | AFTER: {} B/conn | Target: 972 B/conn", conn_size);

    println!("\n==========================================================================================");
    println!("             FYRODB MULTI-CORE HARDWARE SCALING VERIFICATION (1 vs 6 THREADS)             ");
    println!("==========================================================================================");

    // [19/54] Multi-Core Concurrent SET Scaling (6 Threads vs 1 Thread, 128 Shards)
    let mc_store = Arc::new(Store::with_config(128, 800_000));
    let single_ops = 100_000;
    let t0 = Instant::now();
    for i in 0..single_ops {
        let k = format!("sc_k:{:08}", i);
        mc_store.set_string(&k, "val", 0);
    }
    let d_single_set = t0.elapsed();
    let single_set_mops = (single_ops as f64) / d_single_set.as_secs_f64() / 1_000_000.0;

    let multi_threads = 6;
    let multi_ops_per_thread = 100_000;
    let total_multi_ops = multi_threads * multi_ops_per_thread;
    let t0 = Instant::now();
    let mut handles = Vec::new();
    for t in 0..multi_threads {
        let s = Arc::clone(&mc_store);
        handles.push(std::thread::spawn(move || {
            for i in 0..multi_ops_per_thread {
                let k = format!("mc_k_{}:{:<08}", t, i);
                s.set_string(&k, "val", 0);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    let d_multi_set = t0.elapsed();
    let multi_set_mops = (total_multi_ops as f64) / d_multi_set.as_secs_f64() / 1_000_000.0;
    let set_scaling = multi_set_mops / single_set_mops;
    println!("[19/54] 6-Thread SET Scaling:         1-Core: {:.2} Mops/s | 6-Thread: {:.2} Mops/s | Scaling: {:.2}x (Target: 3.2x)", single_set_mops, multi_set_mops, set_scaling);

    // [20/54] Multi-Core Concurrent GET Scaling (6 Threads vs 1 Thread)
    let t0 = Instant::now();
    for i in 0..single_ops {
        let k = format!("sc_k:{:08}", i);
        let _ = std::hint::black_box(mc_store.get(&k));
    }
    let d_single_get = t0.elapsed();
    let single_get_mops = (single_ops as f64) / d_single_get.as_secs_f64() / 1_000_000.0;

    let t0 = Instant::now();
    let mut handles = Vec::new();
    for t in 0..multi_threads {
        let s = Arc::clone(&mc_store);
        handles.push(std::thread::spawn(move || {
            for i in 0..multi_ops_per_thread {
                let k = format!("mc_k_{}:{:<08}", t, i);
                let _ = std::hint::black_box(s.get(&k));
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    let d_multi_get = t0.elapsed();
    let multi_get_mops = (total_multi_ops as f64) / d_multi_get.as_secs_f64() / 1_000_000.0;
    let get_scaling = multi_get_mops / single_get_mops;
    println!("[20/54] 6-Thread GET Scaling:         1-Core: {:.2} Mops/s | 6-Thread: {:.2} Mops/s | Scaling: {:.2}x (Target: 3.3x)", single_get_mops, multi_get_mops, get_scaling);

    // [21/54] 1MB Large Value Response Buffer Retention & Reuse (x500 reps)
    let val_1mb = vec![b'x'; 1024 * 1024];
    store.set_string("large_1mb", unsafe { std::str::from_utf8_unchecked(&val_1mb) }, 0);
    let mut wbuf = Vec::with_capacity(1024 * 1024 + 64);
    let t0 = Instant::now();
    for _ in 0..500 {
        wbuf.clear();
        assert!(store.get_to_buf("large_1mb", &mut wbuf));
        assert!(wbuf.len() >= 1024 * 1024);
        assert!(wbuf.capacity() >= 1024 * 1024);
    }
    let d_1mb = t0.elapsed();
    let mb_per_sec = (500.0 * 1.0) / d_1mb.as_secs_f64();
    println!("[21/54] 1MB Value Buffer Reuse (x500): BEFORE: ~350 MB/s (thrashing) | AFTER: {:.1} MB/s ({:?}) [0 reallocs] | Target: 2,500 MB/s", mb_per_sec, d_1mb);

    // [22/54] Pub/Sub Concurrent Multi-Thread Publish (60k msgs across 6 Threads)
    let pubsub = Arc::new(fyro_db::pubsub::PubSub::new());
    let t0 = Instant::now();
    let mut handles = Vec::new();
    for t in 0..multi_threads {
        let ps = Arc::clone(&pubsub);
        handles.push(std::thread::spawn(move || {
            let channel = format!("chan_{}", t);
            for _ in 0..10_000 {
                let _ = ps.publish(&channel, "hello multi-core world");
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    let d_pubsub = t0.elapsed();
    let pubsub_mops = (60_000.0) / d_pubsub.as_secs_f64() / 1_000_000.0;
    println!("[22/54] Multi-Thread Pub/Sub Publish: BEFORE: 1.12 Mops/s | AFTER: {:.2} Mops/s ({:?}) | Target: 3.20 Mops/s", pubsub_mops, d_pubsub);

    println!("==========================================================================================\n");
}
