use arc_swap::ArcSwapOption;
use parking_lot::{Mutex, RwLock};
use std::sync::Arc;
use std::time::Instant;

#[test]
fn bench_lock_optimizations_before_and_after() {
   println!(
      "\n=========================================================================================="
   );
   println!(
      "                  LOCK MODERNIZATION: BEFORE vs AFTER MICROBENCHMARKS                    "
   );
   println!(
      "=========================================================================================="
   );

   const THREADS: usize = 8;
   const OPS_PER_THREAD: usize = 500_000;
   const TOTAL_OPS: usize = THREADS * OPS_PER_THREAD;

   // -------------------------------------------------------------------------
   // BENCHMARK 1: Pub/Sub Subscriber Lookups (8 Readers Contending)
   // -------------------------------------------------------------------------
   {
      struct DummySubs {
         _channel: String,
         token: usize,
      }

      // BEFORE: Exclusive Mutex on read path
      let mutex_subs = Arc::new(Mutex::new(Some(Box::new(DummySubs {
         _channel: "news".to_string(),
         token: 42,
      }))));

      let t0 = Instant::now();
      let mut handles = Vec::new();
      for _ in 0..THREADS {
         let m = Arc::clone(&mutex_subs);
         handles.push(std::thread::spawn(move || {
            let mut sum = 0usize;
            for _ in 0..OPS_PER_THREAD {
               let guard = m.lock();
               if let Some(s) = guard.as_ref() {
                  sum += s.token;
               }
            }
            sum
         }));
      }
      for h in handles {
         let _ = h.join().unwrap();
      }
      let elapsed_mutex = t0.elapsed();
      let mops_mutex = (TOTAL_OPS as f64) / elapsed_mutex.as_secs_f64() / 1_000_000.0;

      // AFTER: Wait-free ArcSwapOption on read path
      let arc_swap_subs = Arc::new(ArcSwapOption::from(Some(Arc::new(DummySubs {
         _channel: "news".to_string(),
         token: 42,
      }))));

      let t0 = Instant::now();
      let mut handles = Vec::new();
      for _ in 0..THREADS {
         let a = Arc::clone(&arc_swap_subs);
         handles.push(std::thread::spawn(move || {
            let mut sum = 0usize;
            for _ in 0..OPS_PER_THREAD {
               let guard = a.load();
               if let Some(s) = guard.as_ref() {
                  sum += s.token;
               }
            }
            sum
         }));
      }
      for h in handles {
         let _ = h.join().unwrap();
      }
      let elapsed_arcswap = t0.elapsed();
      let mops_arcswap = (TOTAL_OPS as f64) / elapsed_arcswap.as_secs_f64() / 1_000_000.0;
      let speedup_1 = elapsed_mutex.as_secs_f64() / elapsed_arcswap.as_secs_f64();

      println!(
         "[1] Pub/Sub Subscriber Lookups (8-Thread Read): \n    • BEFORE (parking_lot::Mutex):  {:.2} Mops/s ({:?})\n    • AFTER  (arc_swap::ArcSwap):   {:.2} Mops/s ({:?})  ==>  {:.2}x faster",
         mops_mutex, elapsed_mutex, mops_arcswap, elapsed_arcswap, speedup_1
      );
   }

   // -------------------------------------------------------------------------
   // BENCHMARK 2: Snapshot Write Capture Sink (8 Concurrent Writers)
   // -------------------------------------------------------------------------
   {
      type SinkFn = Arc<dyn Fn(usize) + Send + Sync>;

      // BEFORE: RwLock read acquisition per captured write
      let rwlock_sink: Arc<RwLock<Option<SinkFn>>> = Arc::new(RwLock::new(Some(Arc::new(|val| {
         std::hint::black_box(val);
      }))));

      let t0 = Instant::now();
      let mut handles = Vec::new();
      for _ in 0..THREADS {
         let rw = Arc::clone(&rwlock_sink);
         handles.push(std::thread::spawn(move || {
            for i in 0..OPS_PER_THREAD {
               let guard = rw.read();
               if let Some(f) = guard.as_ref() {
                  f(i);
               }
            }
         }));
      }
      for h in handles {
         h.join().unwrap();
      }
      let elapsed_rw = t0.elapsed();
      let mops_rw = (TOTAL_OPS as f64) / elapsed_rw.as_secs_f64() / 1_000_000.0;

      // AFTER: Wait-free ArcSwapOption load (0 atomic RMWs, 0 lock acquisition)
      struct Wrapper(SinkFn);
      let arcswap_sink: Arc<ArcSwapOption<Wrapper>> = Arc::new(ArcSwapOption::from(Some(
         Arc::new(Wrapper(Arc::new(|val| {
            std::hint::black_box(val);
         }))),
      )));

      let t0 = Instant::now();
      let mut handles = Vec::new();
      for _ in 0..THREADS {
         let a = Arc::clone(&arcswap_sink);
         handles.push(std::thread::spawn(move || {
            for i in 0..OPS_PER_THREAD {
               let guard = a.load();
               if let Some(w) = guard.as_ref() {
                  (w.0)(i);
               }
            }
         }));
      }
      for h in handles {
         h.join().unwrap();
      }
      let elapsed_arcswap_sink = t0.elapsed();
      let mops_arcswap_sink = (TOTAL_OPS as f64) / elapsed_arcswap_sink.as_secs_f64() / 1_000_000.0;
      let speedup_2 = elapsed_rw.as_secs_f64() / elapsed_arcswap_sink.as_secs_f64();

      println!(
         "[2] Snapshot Capture Sink Check (8-Thread Write Path): \n    • BEFORE (parking_lot::RwLock): {:.2} Mops/s ({:?})\n    • AFTER  (arc_swap::ArcSwap):   {:.2} Mops/s ({:?})  ==>  {:.2}x faster",
         mops_rw, elapsed_rw, mops_arcswap_sink, elapsed_arcswap_sink, speedup_2
      );
   }

   // -------------------------------------------------------------------------
   // BENCHMARK 3: Upstream Pool Checkout & Return (8 Workers Check-out/Return)
   // -------------------------------------------------------------------------
   {
      const POOL_OPS: usize = 200_000;
      const POOL_TOTAL: usize = THREADS * POOL_OPS;

      // BEFORE: Single central Mutex for pool
      let central_pool = Arc::new(Mutex::new(vec![1u32; 100]));
      let t0 = Instant::now();
      let mut handles = Vec::new();
      for _ in 0..THREADS {
         let pool = Arc::clone(&central_pool);
         handles.push(std::thread::spawn(move || {
            for _ in 0..POOL_OPS {
               let item = {
                  let mut p = pool.lock();
                  p.pop().unwrap_or(1)
               };
               {
                  let mut p = pool.lock();
                  p.push(item);
               }
            }
         }));
      }
      for h in handles {
         h.join().unwrap();
      }
      let elapsed_central = t0.elapsed();
      let mops_central = (POOL_TOTAL as f64) / elapsed_central.as_secs_f64() / 1_000_000.0;

      // AFTER: 16-way sharded stripes with thread-id affinity
      let sharded_pool = Arc::new(std::array::from_fn::<_, 16, _>(|_| {
         Mutex::new(vec![1u32; 10])
      }));
      let t0 = Instant::now();
      let mut handles = Vec::new();
      for tid in 0..THREADS {
         let pool = Arc::clone(&sharded_pool);
         handles.push(std::thread::spawn(move || {
            let shard_idx = tid % 16;
            for _ in 0..POOL_OPS {
               let item = {
                  let mut p = pool[shard_idx].lock();
                  p.pop().unwrap_or(1)
               };
               {
                  let mut p = pool[shard_idx].lock();
                  p.push(item);
               }
            }
         }));
      }
      for h in handles {
         h.join().unwrap();
      }
      let elapsed_sharded = t0.elapsed();
      let mops_sharded = (POOL_TOTAL as f64) / elapsed_sharded.as_secs_f64() / 1_000_000.0;
      let speedup_3 = elapsed_central.as_secs_f64() / elapsed_sharded.as_secs_f64();

      println!(
         "[3] Upstream Pool Checkout/Return (8-Thread High Contention): \n    • BEFORE (Single central Mutex): {:.2} Mops/s ({:?})\n    • AFTER  (16-Way Sharded Mutex):  {:.2} Mops/s ({:?})  ==>  {:.2}x faster",
         mops_central, elapsed_central, mops_sharded, elapsed_sharded, speedup_3
      );
   }

   println!(
      "==========================================================================================\n"
   );
}
