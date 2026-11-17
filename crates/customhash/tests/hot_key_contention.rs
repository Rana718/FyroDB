use customhash::CustomMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

#[test]
fn test_hot_key_contention() {
   let map = Arc::new(CustomMap::<u64>::with_capacity(16, 100));
   let key = "hot_key";
   map.insert_str(key, 0);

   let writers_count = 16;
   let readers_count = 4;
   let ops_per_writer = 50_000;
   let running = Arc::new(AtomicBool::new(true));
   let reads_completed = Arc::new(AtomicUsize::new(0));

   let start = Instant::now();

   // Spawn readers
   let mut reader_handles = Vec::new();
   for _ in 0..readers_count {
      let map = map.clone();
      let running = running.clone();
      let reads = reads_completed.clone();
      reader_handles.push(std::thread::spawn(move || {
         let mut count = 0;
         while running.load(Ordering::Relaxed) {
            if let Some(val) = map.get("hot_key") {
               std::hint::black_box(val);
               count += 1;
            }
         }
         reads.fetch_add(count, Ordering::Relaxed);
      }));
   }

   // Spawn writers
   let mut writer_handles = Vec::new();
   for tid in 0..writers_count {
      let map = map.clone();
      writer_handles.push(std::thread::spawn(move || {
         for i in 0..ops_per_writer {
            map.set_str("hot_key", (tid * ops_per_writer + i) as u64);
         }
      }));
   }

   for h in writer_handles {
      h.join().unwrap();
   }
   running.store(false, Ordering::Relaxed);

   for h in reader_handles {
      h.join().unwrap();
   }

   let elapsed = start.elapsed();
   let total_writes = writers_count * ops_per_writer;
   let total_reads = reads_completed.load(Ordering::Relaxed);
   let writes_mops = (total_writes as f64 / 1_000_000.0) / elapsed.as_secs_f64();
   let reads_mops = (total_reads as f64 / 1_000_000.0) / elapsed.as_secs_f64();

   println!("\n=== HOT KEY CONTENTION TEST (16 Writers, 4 Readers on 'hot_key') ===");
   println!("Elapsed: {:?}", elapsed);
   println!("Writes: {} ({:.2} M/s)", total_writes, writes_mops);
   println!("Reads:  {} ({:.2} M/s)", total_reads, reads_mops);
   println!("Total:  {:.2} M/s\n", writes_mops + reads_mops);
}
