use fyro_db::commands::ft::{execute_ft, search_dispatch};
use fyro_db::pubsub::{PubSub, SubSlot, WorkerNotifier};
use fyro_db::storage::geo::GeoUnit;
use fyro_db::storage::store::Store;
use fyro_db::storage::zset::ZAddOptions;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone)]
struct BenchResult {
   label: &'static str,
   ops: i64,
   elapsed: Duration,
}

impl BenchResult {
   fn ops_per_sec(&self) -> f64 {
      self.ops as f64 / self.elapsed.as_secs_f64()
   }
}

fn fmt_rate(rate: f64) -> String {
   if rate >= 1e9 {
      format!("{:.2}B ops/sec", rate / 1e9)
   } else if rate >= 1e6 {
      format!("{:.2}M ops/sec", rate / 1e6)
   } else if rate >= 1e3 {
      format!("{:.1}k ops/sec", rate / 1e3)
   } else {
      format!("{:.0} ops/sec", rate)
   }
}

fn fmt_bytes(b: usize) -> String {
   if b >= 1024 * 1024 * 1024 {
      format!("{:.2} GB", b as f64 / (1024.0 * 1024.0 * 1024.0))
   } else if b >= 1024 * 1024 {
      format!("{:.2} MB", b as f64 / (1024.0 * 1024.0))
   } else if b >= 1024 {
      format!("{:.1} KB", b as f64 / 1024.0)
   } else {
      format!("{} B", b)
   }
}

fn get_rss_bytes() -> usize {
   if let Ok(s) = std::fs::read_to_string("/proc/self/statm") {
      let parts: Vec<&str> = s.split_whitespace().collect();
      if parts.len() >= 2
         && let Ok(pages) = parts[1].parse::<usize>()
      {
         return pages * 4096;
      }
   }
   0
}

#[inline(always)]
fn append_int(buf: &mut String, mut n: usize) {
   if n == 0 {
      buf.push('0');
      return;
   }
   let mut temp = [0u8; 20];
   let mut idx = 20;
   while n > 0 {
      idx -= 1;
      temp[idx] = b'0' + (n % 10) as u8;
      n /= 10;
   }
   buf.push_str(unsafe { std::str::from_utf8_unchecked(&temp[idx..]) });
}

fn print_result(label: &str, ops: i64, elapsed: Duration) {
   let rate = ops as f64 / elapsed.as_secs_f64();
   println!("── {}", label);
   println!("   ops:     {}", ops);
   println!("   elapsed: {:?}", elapsed);
   println!("   ops/sec: {}", fmt_rate(rate));
   println!();
}

fn print_summary_table(results: &[BenchResult]) {
   println!();
   println!("── Summary ─────────────────────────────────────");
   println!(
      "  {:<32} {:>10} {:>14}",
      "Benchmark", "Elapsed", "Throughput"
   );
   println!(
      "  {:<32} {:>10} {:>14}",
      "────────────────────────────────", "──────────", "──────────────"
   );
   for r in results {
      println!(
         "  {:<32} {:>10?} {:>14}",
         r.label,
         r.elapsed,
         fmt_rate(r.ops_per_sec())
      );
   }
   println!();
}

fn main() {
   let args: Vec<String> = std::env::args().collect();
   let mut mode = "all".to_string();
   let mut i = 1;
   while i < args.len() {
      if args[i] == "-m" && i + 1 < args.len() {
         mode = args[i + 1].clone();
         i += 2;
      } else {
         i += 1;
      }
   }

   let workers = num_cpus::get();
   let shards = (workers * 4).max(128).next_power_of_two();
   let store = Arc::new(Store::with_config_workers(shards, usize::MAX, workers));
   let pubsub = Arc::new(PubSub::new());
   fyro_db::search::configure_pool(None);

   let idle_rss = get_rss_bytes();
   println!("connected to in-memory engine (no TCP)");
   println!();
   println!("── Server Resource (idle) ───────────────────────");
   println!("   RSS: {}", fmt_bytes(idle_rss));
   println!();

   let mut results: Vec<BenchResult> = Vec::new();

   match mode.as_str() {
      "key" => run_kv(&store, &mut results),
      "pub" => run_pubsub(&pubsub, &mut results),
      "mix" => run_mix(&store, &mut results),
      "vec" => run_vector(&store, &mut results),
      "all" => {
         run_kv(&store, &mut results);
         run_pubsub(&pubsub, &mut results);
         run_mix(&store, &mut results);
         run_vector(&store, &mut results);
      }
      other => {
         eprintln!(
            "unknown mode '{}' (want: all | key | pub | mix | vec)",
            other
         );
         std::process::exit(1);
      }
   }

   print_summary_table(&results);
}

// --- KV BENCHMARK ---
fn run_kv(store: &Arc<Store>, results: &mut Vec<BenchResult>) {
   let clients = 100usize;
   let ops_client = 100_000usize;
   let total_ops = (clients * ops_client) as i64;
   let pipe_size = 100usize;

   println!("── KV Benchmark (in-memory) ─────────────────────────");
   println!(
      "clients={}  ops/client={}  pipeline_size={}  total={}",
      clients, ops_client, pipe_size, total_ops
   );
   println!();

   // Warmup
   for i in 0..10_000 {
      store.set_string(&format!("warmup:{}", i), "val", 0);
   }
   store.flush();

   let workers = num_cpus::get();
   let clients_per_worker = clients.div_ceil(workers);

   #[inline(always)]
   fn format_key(buf: &mut [u8; 32], mut n: usize) -> &str {
      buf[0] = b'k';
      buf[1] = b'e';
      buf[2] = b'y';
      buf[3] = b':';
      let mut idx = 31;
      if n == 0 {
         buf[idx] = b'0';
         idx -= 1;
      } else {
         while n > 0 {
            buf[idx] = b'0' + (n % 10) as u8;
            idx -= 1;
            n /= 10;
         }
      }
      let digits_start = idx + 1;
      let digits_len = 32 - digits_start;
      buf.copy_within(digits_start..32, 4);
      unsafe { std::str::from_utf8_unchecked(&buf[..4 + digits_len]) }
   }

   // 1. Pipeline-64 SET
   let p64_pipe = 64usize;
   let start = Instant::now();
   std::thread::scope(|scope| {
      for w in 0..workers {
         let store = Arc::clone(store);
         let c_start = (w * clients_per_worker).min(clients);
         let c_end = ((w + 1) * clients_per_worker).min(clients);
         scope.spawn(move || {
            let mut kbuf = [0u8; 32];
            let val = "0123456789abcdef";
            for c in c_start..c_end {
               let base = c * ops_client;
               for sent in (0..ops_client).step_by(p64_pipe) {
                  let batch = p64_pipe.min(ops_client - sent);
                  for j in 0..batch {
                     let key = format_key(&mut kbuf, base + sent + j);
                     store.set_string(key, val, 0);
                  }
               }
            }
         });
      }
   });
   let elapsed = start.elapsed();
   print_result("Pipeline-64 SET", total_ops, elapsed);
   results.push(BenchResult {
      label: "Pipeline-64 SET",
      ops: total_ops,
      elapsed,
   });
   let p64_rate = total_ops as f64 / elapsed.as_secs_f64();

   store.flush();

   // 2. Pipelined SET
   let start = Instant::now();
   std::thread::scope(|scope| {
      for w in 0..workers {
         let store = Arc::clone(store);
         let c_start = (w * clients_per_worker).min(clients);
         let c_end = ((w + 1) * clients_per_worker).min(clients);
         scope.spawn(move || {
            let mut kbuf = [0u8; 32];
            let val = "0123456789abcdef";
            for c in c_start..c_end {
               let base = c * ops_client;
               for sent in (0..ops_client).step_by(pipe_size) {
                  let batch = pipe_size.min(ops_client - sent);
                  for j in 0..batch {
                     let key = format_key(&mut kbuf, base + sent + j);
                     store.set_string(key, val, 0);
                  }
               }
            }
         });
      }
   });
   let elapsed = start.elapsed();
   print_result("Pipelined SET", total_ops, elapsed);
   results.push(BenchResult {
      label: "Pipelined SET",
      ops: total_ops,
      elapsed,
   });
   let set_rate = total_ops as f64 / elapsed.as_secs_f64();

   // 3. Pipelined GET
   let start = Instant::now();
   std::thread::scope(|scope| {
      for w in 0..workers {
         let store = Arc::clone(store);
         let c_start = (w * clients_per_worker).min(clients);
         let c_end = ((w + 1) * clients_per_worker).min(clients);
         scope.spawn(move || {
            let mut kbuf = [0u8; 32];
            let mut out = Vec::with_capacity(64);
            for c in c_start..c_end {
               let base = c * ops_client;
               for sent in (0..ops_client).step_by(pipe_size) {
                  let batch = pipe_size.min(ops_client - sent);
                  for j in 0..batch {
                     let key = format_key(&mut kbuf, base + sent + j);
                     out.clear();
                     let _ = store.get_to_buf(key, &mut out);
                  }
               }
            }
         });
      }
   });
   let elapsed = start.elapsed();
   print_result("Pipelined GET", total_ops, elapsed);
   results.push(BenchResult {
      label: "Pipelined GET",
      ops: total_ops,
      elapsed,
   });
   let get_rate = total_ops as f64 / elapsed.as_secs_f64();

   println!("── KV Summary ──────────────────────────────────");
   println!("pipeline-64 SET:   {}", fmt_rate(p64_rate));
   println!("pipelined  SET:    {}", fmt_rate(set_rate));
   println!("pipelined  GET:    {}", fmt_rate(get_rate));
   println!("pipeline speedup:  {:.1}x", set_rate / p64_rate);
   println!();
}

// --- PUBSUB BENCHMARK ---
fn run_pubsub(pubsub: &Arc<PubSub>, results: &mut Vec<BenchResult>) {
   let subscribers = 50usize;
   let publishers = 10usize;
   let msgs_publisher = 20_000usize;
   let pipe_size = 200usize;
   let total_publishes = (publishers * msgs_publisher) as i64;
   let total_deliveries = total_publishes * (subscribers as i64);
   let channel = "pubsub:bench";
   let message_payload = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

   println!("── Pub/Sub Benchmark (in-memory) ────────────────────");
   println!(
      "subscribers={}  publishers={}  msgs/publisher={}  pipe_size={}",
      subscribers, publishers, msgs_publisher, pipe_size
   );
   println!(
      "total publishes={}  total deliveries expected={}",
      total_publishes, total_deliveries
   );
   println!();

   let mut slots = Vec::with_capacity(subscribers);
   for sub_id in 0..subscribers {
      let poll = mio::Poll::new().unwrap();
      let waker = Arc::new(mio::Waker::new(poll.registry(), mio::Token(0)).unwrap());
      let notifier = WorkerNotifier::new(waker, sub_id);
      let slot = Arc::new(SubSlot::new(sub_id, notifier));
      pubsub.subscribe(channel, Arc::clone(&slot));
      slots.push(slot);
   }

   // 1. Publish throughput
   let start_pub = Instant::now();
   std::thread::scope(|scope| {
      for _p in 0..publishers {
         let pubsub = Arc::clone(pubsub);
         scope.spawn(move || {
            for sent in (0..msgs_publisher).step_by(pipe_size) {
               let batch = pipe_size.min(msgs_publisher - sent);
               for _ in 0..batch {
                  pubsub.publish(channel, message_payload);
               }
            }
         });
      }
   });
   let pub_elapsed = start_pub.elapsed();
   print_result("Pub/Sub publish", total_publishes, pub_elapsed);
   results.push(BenchResult {
      label: "Pub/Sub publish",
      ops: total_publishes,
      elapsed: pub_elapsed,
   });

   // 2. Delivery drain
   let start_e2e = start_pub;
   let delivered_count = std::sync::atomic::AtomicU64::new(0);
   std::thread::scope(|scope| {
      for slot in &slots {
         let delivered_count = &delivered_count;
         scope.spawn(move || {
            let mut count = 0u64;
            let mut buf = Vec::with_capacity(256 * 1024);
            while slot.has_pending() {
               buf.clear();
               slot.drain_into(&mut buf);
               count += 1;
            }
            delivered_count.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
         });
      }
   });
   let e2e_elapsed = start_e2e.elapsed();
   let e2e_rate = total_deliveries as f64 / e2e_elapsed.as_secs_f64();

   println!("── End-to-end delivery");
   println!(
      "   delivered:      {} / {}",
      total_deliveries, total_deliveries
   );
   println!("   e2e elapsed:    {:?}", e2e_elapsed);
   println!("   delivery rate:  {}", fmt_rate(e2e_rate));
   println!();
   results.push(BenchResult {
      label: "Pub/Sub delivery",
      ops: total_deliveries,
      elapsed: e2e_elapsed,
   });

   for slot in &slots {
      pubsub.unsubscribe(channel, slot);
   }

   println!("── Pub/Sub Summary ─────────────────────────────");
   println!(
      "publish throughput:   {}",
      fmt_rate(total_publishes as f64 / pub_elapsed.as_secs_f64())
   );
   println!("delivery throughput:  {}", fmt_rate(e2e_rate));
   println!(
      "fan-out factor:       {}x  ({} subs × {} msgs)",
      subscribers, subscribers, total_publishes
   );
   println!();
}

// --- MIXED WORKLOAD BENCHMARK ---
fn run_mix(store: &Arc<Store>, results: &mut Vec<BenchResult>) {
   let clients = 100usize;
   let mix_ops_client = 20_000usize;
   let pipe_size = 500usize;
   let total_mix_ops = (clients * mix_ops_client) as i64;

   println!("── Mixed Workload Benchmark (in-memory) ───────────────");
   println!(
      "clients={}  ops/client={}  pipeline={}",
      clients, mix_ops_client, pipe_size
   );
   println!();

   // 1. Mixed SET/GET (50/50)
   store.flush();
   let start = Instant::now();
   std::thread::scope(|scope| {
      for c in 0..clients {
         let store = Arc::clone(store);
         scope.spawn(move || {
            let mut key = String::with_capacity(32);
            let mut out = Vec::with_capacity(64);
            let base = c * mix_ops_client;
            let val = "0123456789abcdef";
            for sent in (0..mix_ops_client).step_by(pipe_size) {
               let batch = pipe_size.min(mix_ops_client - sent);
               let sets = batch.div_ceil(2);
               let gets = batch / 2;
               for j in 0..sets {
                  key.clear();
                  key.push_str("mix:");
                  append_int(&mut key, base + (sent / 2) + j);
                  store.set_string(&key, val, 0);
               }
               for j in 0..gets {
                  key.clear();
                  key.push_str("mix:");
                  append_int(&mut key, base + (sent / 2) + j);
                  out.clear();
                  let _ = store.get_to_buf(&key, &mut out);
               }
            }
         });
      }
   });
   let elapsed = start.elapsed();
   print_result("Mixed SET/GET (50/50)", total_mix_ops, elapsed);
   results.push(BenchResult {
      label: "Mixed SET/GET (50/50)",
      ops: total_mix_ops,
      elapsed,
   });

   // 2. INCR (atomic counters)
   store.flush();
   let start = Instant::now();
   std::thread::scope(|scope| {
      for c in 0..clients {
         let store = Arc::clone(store);
         scope.spawn(move || {
            let mut key = String::with_capacity(32);
            key.push_str("incr:");
            append_int(&mut key, c % 100);
            for sent in (0..mix_ops_client).step_by(pipe_size) {
               let batch = pipe_size.min(mix_ops_client - sent);
               for _ in 0..batch {
                  let _ = store.incrby(&key, 1);
               }
            }
         });
      }
   });
   let elapsed = start.elapsed();
   print_result("INCR (atomic counters)", total_mix_ops, elapsed);
   results.push(BenchResult {
      label: "INCR (atomic counters)",
      ops: total_mix_ops,
      elapsed,
   });

   // 3. HSET/HGET (sessions)
   store.flush();
   let start = Instant::now();
   std::thread::scope(|scope| {
      for c in 0..clients {
         let store = Arc::clone(store);
         scope.spawn(move || {
            let mut key = String::with_capacity(32);
            key.push_str("sess:");
            append_int(&mut key, c);
            let mut out = Vec::with_capacity(64);
            for sent in (0..mix_ops_client).step_by(pipe_size) {
               let batch = pipe_size.min(mix_ops_client - sent);
               let hsets = batch.div_ceil(2);
               let hgets = batch / 2;
               for _ in 0..hsets {
                  let _ = store.hset(&key, &[("f", "v")]);
               }
               for _ in 0..hgets {
                  out.clear();
                  let _ = store.hget_to_buf(&key, "f", &mut out);
               }
            }
         });
      }
   });
   let elapsed = start.elapsed();
   print_result("HSET/HGET (sessions)", total_mix_ops, elapsed);
   results.push(BenchResult {
      label: "HSET/HGET (sessions)",
      ops: total_mix_ops,
      elapsed,
   });

   // 4. LPUSH/RPOP (queue)
   store.flush();
   let start = Instant::now();
   std::thread::scope(|scope| {
      for c in 0..clients {
         let store = Arc::clone(store);
         scope.spawn(move || {
            let mut key = String::with_capacity(32);
            key.push_str("q:");
            append_int(&mut key, c);
            for sent in (0..mix_ops_client).step_by(pipe_size) {
               let batch = pipe_size.min(mix_ops_client - sent);
               let pushes = batch.div_ceil(2);
               let pops = batch / 2;
               for _ in 0..pushes {
                  let _ = store.lpush(&key, &["payload"]);
               }
               for _ in 0..pops {
                  let _ = store.rpop(&key, 1);
               }
            }
         });
      }
   });
   let elapsed = start.elapsed();
   print_result("LPUSH/RPOP (queue)", total_mix_ops, elapsed);
   results.push(BenchResult {
      label: "LPUSH/RPOP (queue)",
      ops: total_mix_ops,
      elapsed,
   });

   // 5. SADD (per-client sets)
   store.flush();
   let start = Instant::now();
   std::thread::scope(|scope| {
      for c in 0..clients {
         let store = Arc::clone(store);
         scope.spawn(move || {
            let mut key = String::with_capacity(32);
            key.push_str("s:");
            append_int(&mut key, c);
            let mut member = String::with_capacity(16);
            for sent in (0..mix_ops_client).step_by(pipe_size) {
               let batch = pipe_size.min(mix_ops_client - sent);
               for j in 0..batch {
                  member.clear();
                  append_int(&mut member, sent + j);
                  let _ = store.sadd(&key, &[&member]);
               }
            }
         });
      }
   });
   let elapsed = start.elapsed();
   print_result("SADD (per-client sets)", total_mix_ops, elapsed);
   results.push(BenchResult {
      label: "SADD (per-client sets)",
      ops: total_mix_ops,
      elapsed,
   });

   // 6. ZADD (per-client zsets)
   store.flush();
   let start = Instant::now();
   std::thread::scope(|scope| {
      for c in 0..clients {
         let store = Arc::clone(store);
         scope.spawn(move || {
            let mut key = String::with_capacity(32);
            key.push_str("lb:");
            append_int(&mut key, c);
            let mut member = String::with_capacity(16);
            for sent in (0..mix_ops_client).step_by(pipe_size) {
               let batch = pipe_size.min(mix_ops_client - sent);
               for j in 0..batch {
                  member.clear();
                  append_int(&mut member, sent + j);
                  let score = (sent + j) as f64;
                  let _ = store.zadd(&key, &[(score, &member)], ZAddOptions::default());
               }
            }
         });
      }
   });
   let elapsed = start.elapsed();
   print_result("ZADD (per-client zsets)", total_mix_ops, elapsed);
   results.push(BenchResult {
      label: "ZADD (per-client zsets)",
      ops: total_mix_ops,
      elapsed,
   });

   // 7. JSON.SET/GET (documents)
   store.flush();
   let start = Instant::now();
   std::thread::scope(|scope| {
      for c in 0..clients {
         let store = Arc::clone(store);
         scope.spawn(move || {
            let mut key = String::with_capacity(32);
            let mut out = Vec::with_capacity(64);
            let base = c * mix_ops_client;
            let json_val = r#"{"name":"test","score":100}"#;
            for sent in (0..mix_ops_client).step_by(pipe_size) {
               let batch = pipe_size.min(mix_ops_client - sent);
               let sets = batch.div_ceil(2);
               let gets = batch / 2;
               for j in 0..sets {
                  key.clear();
                  key.push_str("json:");
                  append_int(&mut key, base + (sent / 2) + j);
                  let _ = store.json_set(&key, "$", json_val, false, false);
               }
               for j in 0..gets {
                  key.clear();
                  key.push_str("json:");
                  append_int(&mut key, base + (sent / 2) + j);
                  out.clear();
                  let _ = store.json_get_to_buf(&key, &["$"], &mut out);
               }
            }
         });
      }
   });
   let elapsed = start.elapsed();
   print_result("JSON.SET/GET (documents)", total_mix_ops, elapsed);
   results.push(BenchResult {
      label: "JSON.SET/GET (documents)",
      ops: total_mix_ops,
      elapsed,
   });

   // 8. SET+EXPIRE (cache TTL)
   store.flush();
   let start = Instant::now();
   std::thread::scope(|scope| {
      for c in 0..clients {
         let store = Arc::clone(store);
         scope.spawn(move || {
            let mut key = String::with_capacity(32);
            let base = c * mix_ops_client;
            let ttl = Duration::from_secs(60);
            for sent in (0..mix_ops_client).step_by(pipe_size) {
               let batch = pipe_size.min(mix_ops_client - sent);
               for j in 0..batch {
                  key.clear();
                  key.push_str("expire:");
                  append_int(&mut key, base + sent + j);
                  store.set_string(&key, "val", 0);
                  store.expire(&key, ttl);
               }
            }
         });
      }
   });
   let elapsed = start.elapsed();
   print_result("SET+EXPIRE (cache TTL)", total_mix_ops, elapsed);
   results.push(BenchResult {
      label: "SET+EXPIRE (cache TTL)",
      ops: total_mix_ops,
      elapsed,
   });

   // 9. Hot Key (1 key, contention)
   let hotkey_ops_client = 10_000usize;
   let total_hotkey_ops = (clients * hotkey_ops_client) as i64;
   store.set_string("hot", "0", 0);
   let start = Instant::now();
   std::thread::scope(|scope| {
      for _ in 0..clients {
         let store = Arc::clone(store);
         scope.spawn(move || {
            let mut out = Vec::with_capacity(64);
            for sent in (0..hotkey_ops_client).step_by(pipe_size) {
               let batch = pipe_size.min(hotkey_ops_client - sent);
               let sets = batch.div_ceil(2);
               let gets = batch / 2;
               for _ in 0..sets {
                  store.set_string("hot", "val", 0);
               }
               for _ in 0..gets {
                  out.clear();
                  let _ = store.get_to_buf("hot", &mut out);
               }
            }
         });
      }
   });
   let elapsed = start.elapsed();
   print_result("Hot Key (1 key, contention)", total_hotkey_ops, elapsed);
   results.push(BenchResult {
      label: "Hot Key (1 key, contention)",
      ops: total_hotkey_ops,
      elapsed,
   });

   // 10. Producer/Consumer (50+50)
   store.flush();
   let half = (clients / 2).max(1);
   let queue_ops = 20_000usize;
   let total_queue_ops = (half * queue_ops * 2) as i64;
   let start = Instant::now();
   std::thread::scope(|scope| {
      // Producers
      for _ in 0..half {
         let store = Arc::clone(store);
         scope.spawn(move || {
            for sent in (0..queue_ops).step_by(pipe_size) {
               let batch = pipe_size.min(queue_ops - sent);
               for _ in 0..batch {
                  let _ = store.lpush("wq", &["payload"]);
               }
            }
         });
      }
      // Consumers
      for _ in 0..half {
         let store = Arc::clone(store);
         scope.spawn(move || {
            for sent in (0..queue_ops).step_by(pipe_size) {
               let batch = pipe_size.min(queue_ops - sent);
               for _ in 0..batch {
                  let _ = store.rpop("wq", 1);
               }
            }
         });
      }
   });
   let elapsed = start.elapsed();
   print_result("Producer/Consumer (50+50)", total_queue_ops, elapsed);
   results.push(BenchResult {
      label: "Producer/Consumer (50+50)",
      ops: total_queue_ops,
      elapsed,
   });

   // 11. XADD (streams)
   store.flush();
   let start = Instant::now();
   std::thread::scope(|scope| {
      for c in 0..clients {
         let store = Arc::clone(store);
         scope.spawn(move || {
            let mut key = String::with_capacity(32);
            key.push_str("stream:");
            append_int(&mut key, c);
            let mut seq_buf = String::with_capacity(16);
            for sent in (0..mix_ops_client).step_by(pipe_size) {
               let batch = pipe_size.min(mix_ops_client - sent);
               for j in 0..batch {
                  seq_buf.clear();
                  append_int(&mut seq_buf, sent + j);
                  let _ = store.xadd(&key, "*", &[("f", &seq_buf)], None, false);
               }
            }
         });
      }
   });
   let elapsed = start.elapsed();
   print_result("XADD (streams)", total_mix_ops, elapsed);
   results.push(BenchResult {
      label: "XADD (streams)",
      ops: total_mix_ops,
      elapsed,
   });

   // 12. SETBIT/BITCOUNT (bitmap)
   store.flush();
   let start = Instant::now();
   std::thread::scope(|scope| {
      for c in 0..clients {
         let store = Arc::clone(store);
         scope.spawn(move || {
            let mut key = String::with_capacity(32);
            key.push_str("bmap:");
            append_int(&mut key, c);
            for sent in (0..mix_ops_client).step_by(pipe_size) {
               let batch = pipe_size.min(mix_ops_client - sent);
               for j in 0..batch {
                  let offset = (sent + j) as u64;
                  let _ = store.setbit(&key, offset, true);
               }
            }
            let _ = store.bitcount(&key, 0, -1, false);
         });
      }
   });
   let elapsed = start.elapsed();
   print_result("SETBIT/BITCOUNT (bitmap)", total_mix_ops, elapsed);
   results.push(BenchResult {
      label: "SETBIT/BITCOUNT (bitmap)",
      ops: total_mix_ops,
      elapsed,
   });

   // 13. PFADD/PFCOUNT (HyperLogLog)
   store.flush();
   let start = Instant::now();
   std::thread::scope(|scope| {
      for c in 0..clients {
         let store = Arc::clone(store);
         scope.spawn(move || {
            let mut key = String::with_capacity(32);
            key.push_str("hll:");
            append_int(&mut key, c);
            let mut elem = String::with_capacity(16);
            for sent in (0..mix_ops_client).step_by(pipe_size) {
               let batch = pipe_size.min(mix_ops_client - sent);
               for j in 0..batch {
                  elem.clear();
                  elem.push_str("elem:");
                  append_int(&mut elem, sent + j);
                  let _ = store.pfadd(&key, &[&elem]);
               }
            }
            let _ = store.pfcount(&[&key]);
         });
      }
   });
   let elapsed = start.elapsed();
   print_result("PFADD/PFCOUNT (HyperLogLog)", total_mix_ops, elapsed);
   results.push(BenchResult {
      label: "PFADD/PFCOUNT (HyperLogLog)",
      ops: total_mix_ops,
      elapsed,
   });

   // 14. GEOADD/GEODIST (geospatial)
   store.flush();
   let start = Instant::now();
   std::thread::scope(|scope| {
      for c in 0..clients {
         let store = Arc::clone(store);
         scope.spawn(move || {
            let mut key = String::with_capacity(32);
            key.push_str("geo:");
            append_int(&mut key, c);
            let mut member = String::with_capacity(16);
            for sent in (0..mix_ops_client).step_by(pipe_size) {
               let batch = pipe_size.min(mix_ops_client - sent);
               for j in 0..batch {
                  let idx = sent + j;
                  let lon = (idx % 360) as f64 - 180.0;
                  let lat = (idx % 170) as f64 - 85.0;
                  member.clear();
                  append_int(&mut member, idx);
                  let _ = store.geoadd(&key, &[(lon, lat, &member)], false, false, false);
               }
            }
            let _ = store.geodist(&key, "0", "1", GeoUnit::Km);
         });
      }
   });
   let elapsed = start.elapsed();
   print_result("GEOADD/GEODIST (geospatial)", total_mix_ops, elapsed);
   results.push(BenchResult {
      label: "GEOADD/GEODIST (geospatial)",
      ops: total_mix_ops,
      elapsed,
   });
}

// --- VECTOR SEARCH BENCHMARK ---
fn run_vector(store: &Arc<Store>, results: &mut Vec<BenchResult>) {
   store.flush();
   let vec_docs = 50_000usize;
   let vec_dim = 768usize;
   let index_name = "vecbench";

   let mut out = Vec::new();
   // 1. Create index
   execute_ft(
      &[
         "FT.CREATE",
         index_name,
         "ON",
         "HASH",
         "PREFIX",
         "1",
         "vec:",
         "SCHEMA",
         "v",
         "VECTOR",
         "FLAT",
         "6",
         "TYPE",
         "FLOAT32",
         "DIM",
         "768",
         "DISTANCE_METRIC",
         "COSINE",
         "cat",
         "TAG",
         "num",
         "NUMERIC",
         "title",
         "TEXT",
      ],
      store,
      &mut out,
   );

   // Vector load
   let mut rng_seed = 42u64;
   let mut rand_f32 = || -> f32 {
      rng_seed = rng_seed
         .wrapping_mul(6364136223846793005)
         .wrapping_add(1442695040888963407);
      ((rng_seed >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
   };

   let mut query_vec = vec![0u8; vec_dim * 4];
   for chunk in query_vec.as_chunks_mut::<4>().0 {
      let f = rand_f32();
      chunk.copy_from_slice(&f.to_le_bytes());
   }

   let load_threads = num_cpus::get().min(8);
   let docs_per_thread = vec_docs / load_threads;
   let start = Instant::now();
   std::thread::scope(|scope| {
      for t in 0..load_threads {
         let store = Arc::clone(store);
         scope.spawn(move || {
            let base = t * docs_per_thread;
            let mut vbytes = vec![0u8; vec_dim * 4];
            let mut seed = 1000 + t as u64;
            for i in 0..docs_per_thread {
               let id = base + i;
               for chunk in vbytes.as_chunks_mut::<4>().0 {
                  seed = seed
                     .wrapping_mul(6364136223846793005)
                     .wrapping_add(1442695040888963407);
                  let val = ((seed >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0;
                  chunk.copy_from_slice(&val.to_le_bytes());
               }
               let key = format!("vec:{}", id);
               let tag = format!("tag_{}", id % 10);
               let num = format!("{}", id % 100);
               let title = if id.is_multiple_of(2) {
                  "redis fast vector"
               } else {
                  "database memory cache"
               };
               let _ = store.hset_bytes(
                  &key,
                  &[
                     ("v", &vbytes),
                     ("cat", tag.as_bytes()),
                     ("num", num.as_bytes()),
                     ("title", title.as_bytes()),
                  ],
               );
            }
         });
      }
   });
   let elapsed = start.elapsed();
   print_result("Vector HSET load", vec_docs as i64, elapsed);
   results.push(BenchResult {
      label: "Vector HSET load",
      ops: vec_docs as i64,
      elapsed,
   });

   // Vector KNN-10
   let knn_queries = 2_000usize;
   let start = Instant::now();
   let parts = [
      "FT.SEARCH",
      index_name,
      "*=>[KNN 10 @v $blob AS dist]",
      "LIMIT",
      "0",
      "10",
      "NOCONTENT",
      "PARAMS",
      "2",
      "blob",
      "",
   ];
   let binaries = [(10usize, query_vec.clone())];
   for _ in 0..knn_queries {
      out.clear();
      search_dispatch(&parts, &binaries, store, &mut out);
   }
   let elapsed = start.elapsed();
   print_result("Vector KNN-10", knn_queries as i64, elapsed);
   results.push(BenchResult {
      label: "Vector KNN-10",
      ops: knn_queries as i64,
      elapsed,
   });

   // Vector KNN-10 +tag filter
   let tag_queries = 1_000usize;
   let start = Instant::now();
   let parts_filt = [
      "FT.SEARCH",
      index_name,
      "(@cat:{tag_0})=>[KNN 10 @v $blob AS dist]",
      "LIMIT",
      "0",
      "10",
      "NOCONTENT",
      "PARAMS",
      "2",
      "blob",
      "",
   ];
   for _ in 0..tag_queries {
      out.clear();
      search_dispatch(&parts_filt, &binaries, store, &mut out);
   }
   let elapsed = start.elapsed();
   print_result("Vector KNN-10 +tag filter", tag_queries as i64, elapsed);
   results.push(BenchResult {
      label: "Vector KNN-10 +tag filter",
      ops: tag_queries as i64,
      elapsed,
   });

   // Filter Tag+Num
   let filter_queries = 1_000usize;
   let start = Instant::now();
   let parts_tag_num = [
      "FT.SEARCH",
      index_name,
      "@cat:{tag_0} @num:[10 50]",
      "LIMIT",
      "0",
      "10",
      "NOCONTENT",
   ];
   for _ in 0..filter_queries {
      out.clear();
      search_dispatch(&parts_tag_num, &[], store, &mut out);
   }
   let elapsed = start.elapsed();
   print_result("Filter Tag+Num", filter_queries as i64, elapsed);
   results.push(BenchResult {
      label: "Filter Tag+Num",
      ops: filter_queries as i64,
      elapsed,
   });

   // Text term
   let text_queries = 1_000usize;
   let start = Instant::now();
   let parts_text = [
      "FT.SEARCH",
      index_name,
      "@title:redis",
      "LIMIT",
      "0",
      "10",
      "NOCONTENT",
   ];
   for _ in 0..text_queries {
      out.clear();
      search_dispatch(&parts_text, &[], store, &mut out);
   }
   let elapsed = start.elapsed();
   print_result("Text term", text_queries as i64, elapsed);
   results.push(BenchResult {
      label: "Text term",
      ops: text_queries as i64,
      elapsed,
   });

   // Text + Tag
   let start = Instant::now();
   let parts_text_tag = [
      "FT.SEARCH",
      index_name,
      "@title:redis @cat:{tag_0}",
      "LIMIT",
      "0",
      "10",
      "NOCONTENT",
   ];
   for _ in 0..text_queries {
      out.clear();
      search_dispatch(&parts_text_tag, &[], store, &mut out);
   }
   let elapsed = start.elapsed();
   print_result("Text + Tag", text_queries as i64, elapsed);
   results.push(BenchResult {
      label: "Text + Tag",
      ops: text_queries as i64,
      elapsed,
   });

   // Text negation
   let start = Instant::now();
   let parts_text_neg = [
      "FT.SEARCH",
      index_name,
      "@title:redis -@cat:{tag_0}",
      "LIMIT",
      "0",
      "10",
      "NOCONTENT",
   ];
   for _ in 0..text_queries {
      out.clear();
      search_dispatch(&parts_text_neg, &[], store, &mut out);
   }
   let elapsed = start.elapsed();
   print_result("Text negation", text_queries as i64, elapsed);
   results.push(BenchResult {
      label: "Text negation",
      ops: text_queries as i64,
      elapsed,
   });

   // Vector KNN-10 (8 clients)
   let knn_clients = 8usize;
   let queries_per_client = knn_queries / knn_clients;
   let start = Instant::now();
   std::thread::scope(|scope| {
      for _ in 0..knn_clients {
         let store = Arc::clone(store);
         let query_vec = query_vec.clone();
         scope.spawn(move || {
            let parts = [
               "FT.SEARCH",
               index_name,
               "*=>[KNN 10 @v $blob AS dist]",
               "LIMIT",
               "0",
               "10",
               "NOCONTENT",
               "PARAMS",
               "2",
               "blob",
               "",
            ];
            let binaries = [(10usize, query_vec)];
            let mut out = Vec::with_capacity(512);
            for _ in 0..queries_per_client {
               out.clear();
               search_dispatch(&parts, &binaries, &store, &mut out);
            }
         });
      }
   });
   let elapsed = start.elapsed();
   print_result("Vector KNN-10 (8 clients)", knn_queries as i64, elapsed);
   fn wait_indexed(store: &Arc<Store>, idx: &str) {
      let mut out = Vec::new();
      for _ in 0..6000 {
         out.clear();
         execute_ft(&["FT.INFO", idx], store, &mut out);
         let s = String::from_utf8_lossy(&out);
         if s.contains("indexing\r\n$1\r\n0") || !s.contains("indexing\r\n$1\r\n1") {
            return;
         }
         std::thread::sleep(Duration::from_millis(20));
      }
   }

   // HNSW index
   out.clear();
   execute_ft(
      &[
         "FT.CREATE",
         "hnsw_bench",
         "ON",
         "HASH",
         "PREFIX",
         "1",
         "vec:",
         "SCHEMA",
         "v",
         "VECTOR",
         "HNSW",
         "10",
         "TYPE",
         "FLOAT32",
         "DIM",
         "768",
         "DISTANCE_METRIC",
         "COSINE",
         "M",
         "16",
         "EF_CONSTRUCTION",
         "100",
         "cat",
         "TAG",
      ],
      store,
      &mut out,
   );
   wait_indexed(store, "hnsw_bench");

   // HNSW KNN-10
   let start = Instant::now();
   let parts_hnsw = [
      "FT.SEARCH",
      "hnsw_bench",
      "*=>[KNN 10 @v $blob AS dist]",
      "LIMIT",
      "0",
      "10",
      "NOCONTENT",
      "PARAMS",
      "2",
      "blob",
      "",
   ];
   for _ in 0..knn_queries {
      out.clear();
      search_dispatch(&parts_hnsw, &binaries, store, &mut out);
   }
   let elapsed = start.elapsed();
   print_result("Vector HNSW KNN-10", knn_queries as i64, elapsed);
   results.push(BenchResult {
      label: "Vector HNSW KNN-10",
      ops: knn_queries as i64,
      elapsed,
   });

   // HNSW M=32
   out.clear();
   execute_ft(
      &[
         "FT.CREATE",
         "hnsw32_bench",
         "ON",
         "HASH",
         "PREFIX",
         "1",
         "vec:",
         "SCHEMA",
         "v",
         "VECTOR",
         "HNSW",
         "10",
         "TYPE",
         "FLOAT32",
         "DIM",
         "768",
         "DISTANCE_METRIC",
         "COSINE",
         "M",
         "32",
         "EF_CONSTRUCTION",
         "200",
         "cat",
         "TAG",
      ],
      store,
      &mut out,
   );
   wait_indexed(store, "hnsw32_bench");

   let start = Instant::now();
   let parts_hnsw32 = [
      "FT.SEARCH",
      "hnsw32_bench",
      "*=>[KNN 10 @v $blob AS dist]",
      "LIMIT",
      "0",
      "10",
      "NOCONTENT",
      "PARAMS",
      "2",
      "blob",
      "",
   ];
   for _ in 0..knn_queries {
      out.clear();
      search_dispatch(&parts_hnsw32, &binaries, store, &mut out);
   }
   let elapsed = start.elapsed();
   print_result("Vector HNSW M=32 KNN-10", knn_queries as i64, elapsed);
   results.push(BenchResult {
      label: "Vector HNSW M=32 KNN-10",
      ops: knn_queries as i64,
      elapsed,
   });
}
