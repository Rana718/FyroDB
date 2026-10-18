use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Instant;

use fyro_db::pubsub::PubSub;
use fyro_db::storage::store::Store;
use fyro_db::worker::run_worker;

fn run_bench_config(num_workers: usize, clients: usize, pipeline: usize, total_ops: usize) {
   let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
   let port = probe.local_addr().unwrap().port();
   drop(probe);

   let store = Arc::new(Store::with_config(32, 2_000_000));
   let pubsub = Arc::new(PubSub::new());

   for idx in 0..num_workers {
      let store_w = Arc::clone(&store);
      let pubsub_w = Arc::clone(&pubsub);
      std::thread::Builder::new()
         .name(format!("worker-{}", idx))
         .spawn(move || {
            run_worker(&store_w, &pubsub_w, port, "127.0.0.1", None, idx);
         })
         .unwrap();
   }

   std::thread::sleep(std::time::Duration::from_millis(500));

   // Warm-up & Pre-populate keys
   let mut warmup = TcpStream::connect(("127.0.0.1", port)).expect("connect warmup");
   warmup.set_nodelay(true).unwrap();
   let mut warmup_r = BufReader::new(warmup.try_clone().unwrap());
   for i in 0..1000 {
      let cmd = format!(
         "*3\r\n$3\r\nSET\r\n$9\r\nperf:{:04}\r\n$16\r\n0123456789abcdef\r\n",
         i
      );
      warmup.write_all(cmd.as_bytes()).unwrap();
      let mut line = String::new();
      warmup_r.read_line(&mut line).unwrap();
      assert_eq!(line.trim_end(), "+OK");
   }
   drop(warmup);

   let req_per_client = total_ops / clients;

   // GET Benchmark
   let start_get = Instant::now();
   let mut handles_get = Vec::with_capacity(clients);
   for client_id in 0..clients {
      handles_get.push(std::thread::spawn(move || {
         let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect client");
         stream.set_nodelay(true).unwrap();
         let mut reader = BufReader::with_capacity(524288, stream.try_clone().unwrap());

         let mut batch_buf = Vec::with_capacity(pipeline * 48);
         let batches = req_per_client / pipeline;

         for b in 0..batches {
            batch_buf.clear();
            for i in 0..pipeline {
               let k = (client_id * 50 + (b * pipeline + i)) % 1000;
               let cmd = format!("*2\r\n$3\r\nGET\r\n$9\r\nperf:{:04}\r\n", k);
               batch_buf.extend_from_slice(cmd.as_bytes());
            }
            stream.write_all(&batch_buf).unwrap();

            let mut l1 = String::with_capacity(32);
            let mut l2 = String::with_capacity(32);
            for _ in 0..pipeline {
               l1.clear();
               l2.clear();
               reader.read_line(&mut l1).unwrap();
               reader.read_line(&mut l2).unwrap();
            }
         }
      }));
   }
   for h in handles_get {
      h.join().unwrap();
   }
   let elapsed_get = start_get.elapsed();
   let qps_get = (total_ops as f64) / elapsed_get.as_secs_f64();

   // SET Benchmark
   let start_set = Instant::now();
   let mut handles_set = Vec::with_capacity(clients);
   for client_id in 0..clients {
      handles_set.push(std::thread::spawn(move || {
         let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect client");
         stream.set_nodelay(true).unwrap();
         let mut reader = BufReader::with_capacity(524288, stream.try_clone().unwrap());

         let mut batch_buf = Vec::with_capacity(pipeline * 64);
         let batches = req_per_client / pipeline;

         for b in 0..batches {
            batch_buf.clear();
            for i in 0..pipeline {
               let k = (client_id * 50 + (b * pipeline + i)) % 1000;
               let cmd = format!(
                  "*3\r\n$3\r\nSET\r\n$9\r\nperf:{:04}\r\n$16\r\n0123456789abcdef\r\n",
                  k
               );
               batch_buf.extend_from_slice(cmd.as_bytes());
            }
            stream.write_all(&batch_buf).unwrap();

            let mut line = String::with_capacity(32);
            for _ in 0..pipeline {
               line.clear();
               reader.read_line(&mut line).unwrap();
            }
         }
      }));
   }
   for h in handles_set {
      h.join().unwrap();
   }
   let elapsed_set = start_set.elapsed();
   let qps_set = (total_ops as f64) / elapsed_set.as_secs_f64();

   println!(
      "WORKERS: {:2} | Clients: {:2} | Pipe: {:4} || GET: {:8.2}M QPS ({:5.1}ms) | SET: {:8.2}M QPS ({:5.1}ms)",
      num_workers,
      clients,
      pipeline,
      qps_get / 1_000_000.0,
      elapsed_get.as_secs_f64() * 1000.0,
      qps_set / 1_000_000.0,
      elapsed_set.as_secs_f64() * 1000.0
   );
}

#[test]
fn bench_targeting_20m_qps() {
   println!("\n==========================================================================");
   println!("               TARGET: 20,000,000+ QPS BENCHMARK");
   println!("==========================================================================");

   // 1. Can 2 workers reach closer to 20M with pipeline 1024?
   run_bench_config(2, 24, 1024, 1_228_800);

   // 2. 4 Workers (Ultra-low core footprint, utilizing only 4 of 32 cores!)
   run_bench_config(4, 24, 512, 1_536_000);
   run_bench_config(4, 32, 512, 1_536_000);

   // 3. 6 Workers
   run_bench_config(6, 36, 512, 1_843_200);

   // 4. 8 Workers (Peak Throughput to push both GET & SET past 20M+)
   run_bench_config(8, 32, 512, 2_048_000);
   run_bench_config(8, 48, 512, 2_457_600);

   println!("==========================================================================\n");
}
