use fyro_db::storage::store::Store;
use std::{sync::Barrier, thread, time::Instant};
#[global_allocator]
static ALLOC: rust_zmalloc::Zmalloc = rust_zmalloc::Zmalloc;
fn main() {
   const THREADS: usize = 12;
   const ROUNDS: usize = 32768;
   const KEYS: usize = 65536;
   let store = Store::with_config_workers(128, usize::MAX, THREADS);
   let keys: Vec<_> = (0..KEYS).map(|i| format!("account:{i:05}")).collect();
   for key in &keys {
      store.set_string(key, "value", 0);
   }
   let gate = Barrier::new(THREADS + 1);
   let elapsed = thread::scope(|s| {
      let mut jobs = Vec::new();
      for worker in 0..THREADS {
         let (store, keys, gate) = (&store, &keys, &gate);
         jobs.push(s.spawn(move || {
            let mut out = Vec::with_capacity(64 * 11 + 16);
            gate.wait();
            for i in 0..ROUNDS {
               let key = keys[(i * 31 + worker * 211) % KEYS].as_bytes();
               let batch = [key; 64];
               out.clear();
               store.write_get_pipeline(&batch, &mut out);
               assert_eq!(out.len(), 704);
               std::hint::black_box(out.as_slice());
            }
         }));
      }
      let start = Instant::now();
      gate.wait();
      for j in jobs {
         j.join().unwrap();
      }
      start.elapsed()
   });
   println!("{}", (ROUNDS * THREADS * 64) as f64 / elapsed.as_secs_f64());
}
