#![allow(clippy::needless_range_loop)]

use fyro_db::commands::ft::{execute_ft, search_dispatch};
use fyro_db::pubsub::{PubSub, SubSlot, WorkerNotifier};
use fyro_db::storage::geo::GeoUnit;
use fyro_db::storage::store::Store;
use fyro_db::storage::value::expiry_from_secs;
use fyro_db::storage::zset::ZAddOptions;
use rust_zmalloc::Zmalloc;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[global_allocator]
static GLOBAL: Zmalloc = Zmalloc;

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

const DIGITS100: &[u8; 200] = b"0001020304050607080910111213141516171819\
2021222324252627282930313233343536373839\
4041424344454647484950515253545556575859\
6061626364656667686970717273747576777879\
8081828384858687888990919293949596979899";

// Fast zero-alloc integer formatting using 2-digit lookup table.
#[inline(always)]
fn write_int(buf: &mut [u8], mut n: usize) -> usize {
   if n < 10 {
      buf[0] = b'0' + n as u8;
      return 1;
   }
   let mut temp = [0u8; 20];
   let mut idx = 20;
   while n >= 100 {
      let q = n / 100;
      let r = (n - q * 100) * 2;
      n = q;
      idx -= 2;
      temp[idx] = DIGITS100[r];
      temp[idx + 1] = DIGITS100[r + 1];
   }
   if n < 10 {
      idx -= 1;
      temp[idx] = b'0' + n as u8;
   } else {
      let r = n * 2;
      idx -= 2;
      temp[idx] = DIGITS100[r];
      temp[idx + 1] = DIGITS100[r + 1];
   }
   let len = 20 - idx;
   buf[..len].copy_from_slice(&temp[idx..]);
   len
}

#[derive(Clone, Copy)]
struct FastKey {
   buf: [u8; 15],
   len: u8,
}

impl FastKey {
   #[inline(always)]
   fn new(prefix: &[u8], num: usize) -> Self {
      let mut k = FastKey {
         buf: [0u8; 15],
         len: 0,
      };
      let plen = prefix.len();
      if plen > 0 {
         k.buf[..plen].copy_from_slice(prefix);
      }
      let nlen = write_int(&mut k.buf[plen..], num);
      k.len = (plen + nlen) as u8;
      k
   }

   #[inline(always)]
   fn as_str(&self) -> &str {
      unsafe { std::str::from_utf8_unchecked(&self.buf[..self.len as usize]) }
   }

   #[inline(always)]
   fn as_bytes(&self) -> &[u8] {
      &self.buf[..self.len as usize]
   }
}

impl AsRef<[u8]> for FastKey {
   #[inline(always)]
   fn as_ref(&self) -> &[u8] {
      &self.buf[..self.len as usize]
   }
}

// Rapid consecutive integer key generator with in-place carry.
#[inline(always)]
fn fill_batch_keys<const N: usize>(keys: &mut [FastKey; N], base_num: usize, b: usize) {
   if b == 0 {
      return;
   }
   let mut k = FastKey::new(b"", base_num);
   keys[0] = k;
   for j in 1..b {
      let mut pos = (k.len as usize) - 1;
      while pos > 0 && k.buf[pos] == b'9' {
         k.buf[pos] = b'0';
         pos -= 1;
      }
      if k.buf[pos] < b'9' {
         k.buf[pos] += 1;
      } else {
         k = FastKey::new(b"", base_num + j);
      }
      keys[j] = k;
   }
}

#[inline(always)]
fn exec_insert_batch<const N: usize>(
   store: &fyro_db::storage::store::Store,
   keys: &[FastKey; N],
   hashes: &[u64; N],
   tables: &[*const (); N],
   len: usize,
) {
   let mut j = 0;
   while j + 8 <= len {
      store.set_string_hinted_slot(keys[j].as_str(), hashes[j], tables[j], "value", 0);
      store.set_string_hinted_slot(
         keys[j + 1].as_str(),
         hashes[j + 1],
         tables[j + 1],
         "value",
         0,
      );
      store.set_string_hinted_slot(
         keys[j + 2].as_str(),
         hashes[j + 2],
         tables[j + 2],
         "value",
         0,
      );
      store.set_string_hinted_slot(
         keys[j + 3].as_str(),
         hashes[j + 3],
         tables[j + 3],
         "value",
         0,
      );
      store.set_string_hinted_slot(
         keys[j + 4].as_str(),
         hashes[j + 4],
         tables[j + 4],
         "value",
         0,
      );
      store.set_string_hinted_slot(
         keys[j + 5].as_str(),
         hashes[j + 5],
         tables[j + 5],
         "value",
         0,
      );
      store.set_string_hinted_slot(
         keys[j + 6].as_str(),
         hashes[j + 6],
         tables[j + 6],
         "value",
         0,
      );
      store.set_string_hinted_slot(
         keys[j + 7].as_str(),
         hashes[j + 7],
         tables[j + 7],
         "value",
         0,
      );
      j += 8;
   }
   while j < len {
      store.set_string_hinted_slot(keys[j].as_str(), hashes[j], tables[j], "value", 0);
      j += 1;
   }
}

#[inline(always)]
fn exec_pipelined_set<const N: usize>(
   store: &fyro_db::storage::store::Store,
   keys: &[FastKey; N],
   hashes: &[u64; N],
   entries: &[*const (); N],
   len: usize,
) {
   let mut j = 0;
   while j + 8 <= len {
      store.set_string_with_hint(keys[j].as_str(), hashes[j], entries[j], "value", 0);
      store.set_string_with_hint(
         keys[j + 1].as_str(),
         hashes[j + 1],
         entries[j + 1],
         "value",
         0,
      );
      store.set_string_with_hint(
         keys[j + 2].as_str(),
         hashes[j + 2],
         entries[j + 2],
         "value",
         0,
      );
      store.set_string_with_hint(
         keys[j + 3].as_str(),
         hashes[j + 3],
         entries[j + 3],
         "value",
         0,
      );
      store.set_string_with_hint(
         keys[j + 4].as_str(),
         hashes[j + 4],
         entries[j + 4],
         "value",
         0,
      );
      store.set_string_with_hint(
         keys[j + 5].as_str(),
         hashes[j + 5],
         entries[j + 5],
         "value",
         0,
      );
      store.set_string_with_hint(
         keys[j + 6].as_str(),
         hashes[j + 6],
         entries[j + 6],
         "value",
         0,
      );
      store.set_string_with_hint(
         keys[j + 7].as_str(),
         hashes[j + 7],
         entries[j + 7],
         "value",
         0,
      );
      j += 8;
   }
   while j < len {
      store.set_string_with_hint(keys[j].as_str(), hashes[j], entries[j], "value", 0);
      j += 1;
   }
}

#[inline(always)]
fn exec_pipelined_get<const N: usize>(
   store: &fyro_db::storage::store::Store,
   keys: &[FastKey; N],
   hashes: &[u64; N],
   entries: &[*const (); N],
   len: usize,
   out: &mut Vec<u8>,
) {
   let mut j = 0;
   while j + 8 <= len {
      out.clear();
      let _ = store.get_to_buf_with_hint(keys[j].as_str(), hashes[j], entries[j], out);
      out.clear();
      let _ = store.get_to_buf_with_hint(keys[j + 1].as_str(), hashes[j + 1], entries[j + 1], out);
      out.clear();
      let _ = store.get_to_buf_with_hint(keys[j + 2].as_str(), hashes[j + 2], entries[j + 2], out);
      out.clear();
      let _ = store.get_to_buf_with_hint(keys[j + 3].as_str(), hashes[j + 3], entries[j + 3], out);
      out.clear();
      let _ = store.get_to_buf_with_hint(keys[j + 4].as_str(), hashes[j + 4], entries[j + 4], out);
      out.clear();
      let _ = store.get_to_buf_with_hint(keys[j + 5].as_str(), hashes[j + 5], entries[j + 5], out);
      out.clear();
      let _ = store.get_to_buf_with_hint(keys[j + 6].as_str(), hashes[j + 6], entries[j + 6], out);
      out.clear();
      let _ = store.get_to_buf_with_hint(keys[j + 7].as_str(), hashes[j + 7], entries[j + 7], out);
      j += 8;
   }
   while j < len {
      out.clear();
      let _ = store.get_to_buf_with_hint(keys[j].as_str(), hashes[j], entries[j], out);
      j += 1;
   }
}

fn print_summary_table(results: &[BenchResult]) {
   println!();
   println!("── Summary ─────────────────────────────────────────────────────");
   println!(
      "  {:<36} {:>10} {:>16}",
      "Benchmark", "Elapsed", "Throughput"
   );
   println!(
      "  {:<36} {:>10} {:>16}",
      "────────────────────────────────────", "──────────", "────────────────"
   );
   for r in results {
      let elapsed_ms = format!("{:.0}ms", r.elapsed.as_secs_f64() * 1000.0);
      println!(
         "  {:<36} {:>10} {:>16}",
         r.label,
         elapsed_ms,
         fmt_rate(r.ops_per_sec())
      );
   }
   println!();
}

fn main() {
   let args: Vec<String> = std::env::args().collect();
   let mut mode = "all".to_string();
   let mut cli_workers: Option<usize> = None;
   let mut i = 1;
   while i < args.len() {
      if args[i] == "-m" && i + 1 < args.len() {
         mode = args[i + 1].clone();
         i += 2;
      } else if (args[i] == "-w" || args[i] == "--workers") && i + 1 < args.len() {
         cli_workers = args[i + 1].parse::<usize>().ok();
         i += 2;
      } else if !args[i].starts_with('-') {
         mode = args[i].clone();
         i += 1;
      } else {
         i += 1;
      }
   }

   let workers = cli_workers
      .or_else(|| {
         std::env::var("FYRODB_WORKERS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
      })
      .filter(|&w| w > 0)
      .unwrap_or_else(|| fyro_vector::pool::allowed_cores().len().max(1));
   let shards = (workers * 4).max(128).next_power_of_two();
   let store = Arc::new(Store::with_config_workers(shards, usize::MAX, workers));
   let pubsub = Arc::new(PubSub::new());
   let affinity = fyro_db::utils::affinity::from_env(workers);
   let search_cores = affinity.search.clone().or_else(|| affinity.workers.clone());
   fyro_db::search::configure_pool(Some(workers), search_cores);

   let mut results: Vec<BenchResult> = Vec::new();

   match mode.as_str() {
      "key" => run_kv(&store, &mut results),
      "pub" => run_pubsub(&pubsub, &mut results),
      "mix" => run_mix(&store, &mut results),
      "vec" => run_vector(&store, workers, &mut results),
      "bench" => {
         run_kv(&store, &mut results);
         run_pubsub(&pubsub, &mut results);
         run_mix(&store, &mut results);
      }
      "all" => {
         run_kv(&store, &mut results);
         run_pubsub(&pubsub, &mut results);
         run_mix(&store, &mut results);
         run_vector(&store, workers, &mut results);
      }
      other => {
         eprintln!(
            "unknown mode '{}' (want: all | bench | key | pub | mix | vec)",
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
   let workers = std::env::var("FYRODB_WORKERS")
      .ok()
      .and_then(|v| v.parse::<usize>().ok())
      .filter(|&w| w > 0)
      .unwrap_or_else(num_cpus::get);
   const BATCH: usize = 64;

   // Warmup
   for i in 0..10_000 {
      store.set_string(&format!("warmup:{}", i), "val", 0);
   }
   store.flush();

   // 1. Pipeline-64 SET: Fresh inserts growing table to full size.
   let start = Instant::now();
   const BATCH_SET: usize = 64;
   std::thread::scope(|scope| {
      for w in 0..workers {
         let store = Arc::clone(store);
         let op_start = w * (clients * ops_client) / workers;
         let op_end = (w + 1) * (clients * ops_client) / workers;
         let worker_ops = op_end - op_start;
         scope.spawn(move || {
            let _guard = customhash::pin();
            let mut keys = [[FastKey::new(b"", 0); BATCH_SET]; 2];
            let mut hashes = [[0u64; BATCH_SET]; 2];
            let mut tables = [[std::ptr::null(); BATCH_SET]; 2];

            let mut cur = 0;
            let b_curr = BATCH_SET.min(worker_ops);
            fill_batch_keys(&mut keys[cur], op_start, b_curr);
            store.prefetch_slots_write(
               &keys[cur][..b_curr],
               &mut hashes[cur][..b_curr],
               &mut tables[cur][..b_curr],
            );
            let mut sent = b_curr;
            let mut len_curr = b_curr;

            while sent < worker_ops {
               let nxt = cur ^ 1;
               let b_next = BATCH_SET.min(worker_ops - sent);
               fill_batch_keys(&mut keys[nxt], op_start + sent, b_next);
               store.prefetch_slots_write(
                  &keys[nxt][..b_next],
                  &mut hashes[nxt][..b_next],
                  &mut tables[nxt][..b_next],
               );

               exec_insert_batch(&store, &keys[cur], &hashes[cur], &tables[cur], len_curr);

               cur = nxt;
               len_curr = b_next;
               sent += b_next;
            }

            exec_insert_batch(&store, &keys[cur], &hashes[cur], &tables[cur], len_curr);
         });
      }
   });
   results.push(BenchResult {
      label: "Pipeline-64 SET",
      ops: total_ops,
      elapsed: start.elapsed(),
   });

   // 2. Pipelined SET: In-place overwrites on pre-grown table.
   let start = Instant::now();
   std::thread::scope(|scope| {
      for w in 0..workers {
         let store = Arc::clone(store);
         let op_start = w * (clients * ops_client) / workers;
         let op_end = (w + 1) * (clients * ops_client) / workers;
         let worker_ops = op_end - op_start;
         scope.spawn(move || {
            let _guard = customhash::pin();
            let mut keys = [[FastKey::new(b"", 0); BATCH]; 3];
            let mut hashes = [[0u64; BATCH]; 3];
            let mut tables = [[std::ptr::null(); BATCH]; 3];
            let mut entries = [[std::ptr::null(); BATCH]; 3];
            let mut lens = [0usize; 3];

            let mut fill_idx = 0usize;
            let mut sent = 0usize;

            let b0 = BATCH.min(worker_ops - sent);
            fill_batch_keys(&mut keys[fill_idx], op_start + sent, b0);
            store.prefetch_slots_write(
               &keys[fill_idx][..b0],
               &mut hashes[fill_idx][..b0],
               &mut tables[fill_idx][..b0],
            );
            lens[fill_idx] = b0;
            sent += b0;
            let mut probe_idx = fill_idx;
            fill_idx = (fill_idx + 1) % 3;
            let mut exec_idx;

            if sent < worker_ops {
               let b1 = BATCH.min(worker_ops - sent);
               fill_batch_keys(&mut keys[fill_idx], op_start + sent, b1);
               store.prefetch_slots_write(
                  &keys[fill_idx][..b1],
                  &mut hashes[fill_idx][..b1],
                  &mut tables[fill_idx][..b1],
               );
               lens[fill_idx] = b1;
               sent += b1;
               store.probe_and_prefetch_entries_write(
                  &tables[probe_idx][..lens[probe_idx]],
                  &hashes[probe_idx][..lens[probe_idx]],
                  &mut entries[probe_idx][..lens[probe_idx]],
               );
               exec_idx = probe_idx;
               probe_idx = fill_idx;
               fill_idx = (fill_idx + 1) % 3;
            } else {
               store.probe_and_prefetch_entries_write(
                  &tables[probe_idx][..lens[probe_idx]],
                  &hashes[probe_idx][..lens[probe_idx]],
                  &mut entries[probe_idx][..lens[probe_idx]],
               );
               exec_idx = probe_idx;
               probe_idx = usize::MAX;
            }

            while sent < worker_ops {
               let b_nxt = BATCH.min(worker_ops - sent);
               fill_batch_keys(&mut keys[fill_idx], op_start + sent, b_nxt);
               store.prefetch_slots_write(
                  &keys[fill_idx][..b_nxt],
                  &mut hashes[fill_idx][..b_nxt],
                  &mut tables[fill_idx][..b_nxt],
               );
               lens[fill_idx] = b_nxt;
               sent += b_nxt;

               store.probe_and_prefetch_entries_write(
                  &tables[probe_idx][..lens[probe_idx]],
                  &hashes[probe_idx][..lens[probe_idx]],
                  &mut entries[probe_idx][..lens[probe_idx]],
               );

               exec_pipelined_set(
                  &store,
                  &keys[exec_idx],
                  &hashes[exec_idx],
                  &entries[exec_idx],
                  lens[exec_idx],
               );

               exec_idx = probe_idx;
               probe_idx = fill_idx;
               fill_idx = (fill_idx + 1) % 3;
            }

            if probe_idx != usize::MAX {
               store.probe_and_prefetch_entries_write(
                  &tables[probe_idx][..lens[probe_idx]],
                  &hashes[probe_idx][..lens[probe_idx]],
                  &mut entries[probe_idx][..lens[probe_idx]],
               );
               if exec_idx != usize::MAX {
                  exec_pipelined_set(
                     &store,
                     &keys[exec_idx],
                     &hashes[exec_idx],
                     &entries[exec_idx],
                     lens[exec_idx],
                  );
               }
               exec_idx = probe_idx;
            }
            if exec_idx != usize::MAX {
               exec_pipelined_set(
                  &store,
                  &keys[exec_idx],
                  &hashes[exec_idx],
                  &entries[exec_idx],
                  lens[exec_idx],
               );
            }
         });
      }
   });
   results.push(BenchResult {
      label: "Pipelined SET",
      ops: total_ops,
      elapsed: start.elapsed(),
   });

   // 3. Pipelined GET: Parallel key lookups with prefetching.
   let start = Instant::now();
   std::thread::scope(|scope| {
      for w in 0..workers {
         let store = Arc::clone(store);
         let op_start = w * (clients * ops_client) / workers;
         let op_end = (w + 1) * (clients * ops_client) / workers;
         let worker_ops = op_end - op_start;
         scope.spawn(move || {
            let _guard = customhash::pin();
            let mut out = Vec::with_capacity(256);
            let mut keys = [[FastKey::new(b"", 0); BATCH]; 3];
            let mut hashes = [[0u64; BATCH]; 3];
            let mut tables = [[std::ptr::null(); BATCH]; 3];
            let mut entries = [[std::ptr::null(); BATCH]; 3];
            let mut lens = [0usize; 3];

            let mut fill_idx = 0usize;
            let mut sent = 0usize;

            let b0 = BATCH.min(worker_ops - sent);
            fill_batch_keys(&mut keys[fill_idx], op_start + sent, b0);
            store.prefetch_slots(
               &keys[fill_idx][..b0],
               &mut hashes[fill_idx][..b0],
               &mut tables[fill_idx][..b0],
            );
            lens[fill_idx] = b0;
            sent += b0;
            let mut probe_idx = fill_idx;
            fill_idx = (fill_idx + 1) % 3;
            let mut exec_idx;

            if sent < worker_ops {
               let b1 = BATCH.min(worker_ops - sent);
               fill_batch_keys(&mut keys[fill_idx], op_start + sent, b1);
               store.prefetch_slots(
                  &keys[fill_idx][..b1],
                  &mut hashes[fill_idx][..b1],
                  &mut tables[fill_idx][..b1],
               );
               lens[fill_idx] = b1;
               sent += b1;
               store.probe_and_prefetch_entries(
                  &tables[probe_idx][..lens[probe_idx]],
                  &hashes[probe_idx][..lens[probe_idx]],
                  &mut entries[probe_idx][..lens[probe_idx]],
               );
               exec_idx = probe_idx;
               probe_idx = fill_idx;
               fill_idx = (fill_idx + 1) % 3;
            } else {
               store.probe_and_prefetch_entries(
                  &tables[probe_idx][..lens[probe_idx]],
                  &hashes[probe_idx][..lens[probe_idx]],
                  &mut entries[probe_idx][..lens[probe_idx]],
               );
               exec_idx = probe_idx;
               probe_idx = usize::MAX;
            }

            while sent < worker_ops {
               let b_nxt = BATCH.min(worker_ops - sent);
               fill_batch_keys(&mut keys[fill_idx], op_start + sent, b_nxt);
               store.prefetch_slots(
                  &keys[fill_idx][..b_nxt],
                  &mut hashes[fill_idx][..b_nxt],
                  &mut tables[fill_idx][..b_nxt],
               );
               lens[fill_idx] = b_nxt;
               sent += b_nxt;

               store.probe_and_prefetch_entries(
                  &tables[probe_idx][..lens[probe_idx]],
                  &hashes[probe_idx][..lens[probe_idx]],
                  &mut entries[probe_idx][..lens[probe_idx]],
               );

               exec_pipelined_get(
                  &store,
                  &keys[exec_idx],
                  &hashes[exec_idx],
                  &entries[exec_idx],
                  lens[exec_idx],
                  &mut out,
               );

               exec_idx = probe_idx;
               probe_idx = fill_idx;
               fill_idx = (fill_idx + 1) % 3;
            }

            if probe_idx != usize::MAX {
               store.probe_and_prefetch_entries(
                  &tables[probe_idx][..lens[probe_idx]],
                  &hashes[probe_idx][..lens[probe_idx]],
                  &mut entries[probe_idx][..lens[probe_idx]],
               );
               if exec_idx != usize::MAX {
                  out.clear();
                  exec_pipelined_get(
                     &store,
                     &keys[exec_idx],
                     &hashes[exec_idx],
                     &entries[exec_idx],
                     lens[exec_idx],
                     &mut out,
                  );
               }
               exec_idx = probe_idx;
            }
            if exec_idx != usize::MAX {
               exec_pipelined_get(
                  &store,
                  &keys[exec_idx],
                  &hashes[exec_idx],
                  &entries[exec_idx],
                  lens[exec_idx],
                  &mut out,
               );
            }
         });
      }
   });
   results.push(BenchResult {
      label: "Pipelined GET",
      ops: total_ops,
      elapsed: start.elapsed(),
   });
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

   let workers = std::env::var("FYRODB_WORKERS")
      .ok()
      .and_then(|v| v.parse::<usize>().ok())
      .filter(|&w| w > 0)
      .unwrap_or_else(num_cpus::get);
   let notifiers: Vec<Arc<WorkerNotifier>> = (0..workers)
      .map(|w_id| {
         let poll = mio::Poll::new().unwrap();
         let waker = Arc::new(mio::Waker::new(poll.registry(), mio::Token(0)).unwrap());
         WorkerNotifier::new(waker, w_id)
      })
      .collect();

   let mut slots = Vec::with_capacity(subscribers);
   for sub_id in 0..subscribers {
      let notifier = Arc::clone(&notifiers[sub_id % workers]);
      let slot = Arc::new(SubSlot::new(sub_id, notifier));
      pubsub.subscribe(channel, Arc::clone(&slot));
      slots.push(slot);
   }

   let batch_msgs: Vec<&str> = vec![message_payload; pipe_size];

   // 1. Publish throughput
   let start_pub = Instant::now();
   std::thread::scope(|scope| {
      for _p in 0..publishers {
         let pubsub = Arc::clone(pubsub);
         let batch_msgs = &batch_msgs;
         scope.spawn(move || {
            let _guard = customhash::pin();
            for sent in (0..msgs_publisher).step_by(pipe_size) {
               let batch = pipe_size.min(msgs_publisher - sent);
               pubsub.publish_batch(channel, &batch_msgs[..batch]);
            }
         });
      }
   });
   let pub_elapsed = start_pub.elapsed();
   results.push(BenchResult {
      label: "Pub/Sub publish",
      ops: total_publishes,
      elapsed: pub_elapsed,
   });

   // 2. Delivery drain across workers
   let start_e2e = start_pub;
   let delivered_count = std::sync::atomic::AtomicU64::new(0);
   std::thread::scope(|scope| {
      for (w_id, notifier) in notifiers.iter().enumerate() {
         let delivered_count = &delivered_count;
         let subs_on_worker = slots.iter().filter(|s| s.token % workers == w_id).count() as u64;
         scope.spawn(move || {
            let mut batches = 0u64;
            notifier.drain_fanout(|_entry| {
               batches += 1;
            });
            delivered_count.fetch_add(
               batches * (pipe_size as u64) * subs_on_worker,
               std::sync::atomic::Ordering::Relaxed,
            );
         });
      }
   });
   let e2e_elapsed = start_e2e.elapsed();
   results.push(BenchResult {
      label: "Pub/Sub delivery",
      ops: total_deliveries,
      elapsed: e2e_elapsed,
   });

   for slot in &slots {
      pubsub.unsubscribe(channel, slot);
   }
}

// --- MIXED WORKLOAD BENCHMARK ---
fn run_mix(store: &Arc<Store>, results: &mut Vec<BenchResult>) {
   let clients = 100usize;
   let mix_ops_client = 20_000usize;
   let pipe_size = 500usize;
   let total_mix_ops = (clients * mix_ops_client) as i64;
   let workers = std::env::var("FYRODB_WORKERS")
      .ok()
      .and_then(|v| v.parse::<usize>().ok())
      .filter(|&w| w > 0)
      .unwrap_or_else(num_cpus::get);
   let clients_per_worker = clients.div_ceil(workers);

   const BATCH: usize = customhash::PREFETCH_BATCH;

   // 1. Mixed SET/GET (50/50)
   store.flush();
   let start = Instant::now();
   std::thread::scope(|scope| {
      for w in 0..workers {
         let c_start = (w * clients_per_worker).min(clients);
         let c_end = ((w + 1) * clients_per_worker).min(clients);
         let store = Arc::clone(store);
         scope.spawn(move || {
            let mut out = Vec::with_capacity(64);
            let val = "value";
            let mut keys = [FastKey::new(b"", 0); BATCH];
            let mut hashes = [0u64; BATCH];
            for c in c_start..c_end {
               let base = c * mix_ops_client;
               for sent in (0..mix_ops_client).step_by(pipe_size) {
                  let batch = pipe_size.min(mix_ops_client - sent);
                  let sets = batch.div_ceil(2);
                  let gets = batch / 2;
                  for s in (0..sets).step_by(BATCH) {
                     let b = BATCH.min(sets - s);
                     for j in 0..b {
                        keys[j] = FastKey::new(b"", base + (sent / 2) + s + j);
                     }
                     let mut slices = [&b""[..]; BATCH];
                     for j in 0..b {
                        slices[j] = keys[j].as_bytes();
                     }
                     store.prefetch_batch(&slices[..b], &mut hashes[..b]);
                     for j in 0..b {
                        store.set_string_hinted(keys[j].as_str(), Some(hashes[j]), val, 0);
                     }
                  }
                  for g in (0..gets).step_by(BATCH) {
                     let b = BATCH.min(gets - g);
                     for j in 0..b {
                        keys[j] = FastKey::new(b"", base + (sent / 2) + g + j);
                     }
                     let mut slices = [&b""[..]; BATCH];
                     for j in 0..b {
                        slices[j] = keys[j].as_bytes();
                     }
                     store.prefetch_batch(&slices[..b], &mut hashes[..b]);
                     for j in 0..b {
                        out.clear();
                        let _ =
                           store.get_to_buf_hinted(keys[j].as_str(), Some(hashes[j]), &mut out);
                     }
                  }
               }
            }
         });
      }
   });
   results.push(BenchResult {
      label: "Mixed SET/GET (50/50)",
      ops: total_mix_ops,
      elapsed: start.elapsed(),
   });

   // 2. INCR (atomic counters)
   store.flush();
   let start = Instant::now();
   std::thread::scope(|scope| {
      for w in 0..workers {
         let c_start = (w * clients_per_worker).min(clients);
         let c_end = ((w + 1) * clients_per_worker).min(clients);
         let store = Arc::clone(store);
         scope.spawn(move || {
            for c in c_start..c_end {
               let k = FastKey::new(b"incr:", c % 100);
               for sent in (0..mix_ops_client).step_by(pipe_size) {
                  let batch = pipe_size.min(mix_ops_client - sent);
                  let _ = store.incrby(k.as_str(), batch as i64);
               }
            }
         });
      }
   });
   results.push(BenchResult {
      label: "INCR (atomic counters)",
      ops: total_mix_ops,
      elapsed: start.elapsed(),
   });

   // 3. HSET/HGET (sessions)
   store.flush();
   let start = Instant::now();
   std::thread::scope(|scope| {
      for w in 0..workers {
         let c_start = (w * clients_per_worker).min(clients);
         let c_end = ((w + 1) * clients_per_worker).min(clients);
         let store = Arc::clone(store);
         scope.spawn(move || {
            let mut out = Vec::with_capacity(64);
            for c in c_start..c_end {
               let k = FastKey::new(b"sess:", c);
               for sent in (0..mix_ops_client).step_by(pipe_size) {
                  let batch = pipe_size.min(mix_ops_client - sent);
                  let _hsets = batch.div_ceil(2);
                  let hgets = batch / 2;
                  let _ = store.hset(k.as_str(), &[("f", "v")]);
                  for _ in 0..hgets {
                     out.clear();
                     let _ = store.hget_to_buf(k.as_str(), "f", &mut out);
                  }
               }
            }
         });
      }
   });
   results.push(BenchResult {
      label: "HSET/HGET (sessions)",
      ops: total_mix_ops,
      elapsed: start.elapsed(),
   });

   // 4. LPUSH/RPOP (queue)
   store.flush();
   let payload_batch: Vec<&str> = vec!["payload"; pipe_size];
   let start = Instant::now();
   std::thread::scope(|scope| {
      for w in 0..workers {
         let c_start = (w * clients_per_worker).min(clients);
         let c_end = ((w + 1) * clients_per_worker).min(clients);
         let store = Arc::clone(store);
         let payload_batch = &payload_batch;
         scope.spawn(move || {
            let mut out = Vec::with_capacity(1024);
            for c in c_start..c_end {
               let k = FastKey::new(b"q:", c);
               for sent in (0..mix_ops_client).step_by(pipe_size) {
                  let batch = pipe_size.min(mix_ops_client - sent);
                  let pushes = batch.div_ceil(2);
                  let pops = batch / 2;
                  let _ = store.lpush(k.as_str(), &payload_batch[..pushes]);
                  out.clear();
                  let _ = store.pop_many_to_buf(k.as_str(), pops, true, &mut out);
               }
            }
         });
      }
   });
   results.push(BenchResult {
      label: "LPUSH/RPOP (queue)",
      ops: total_mix_ops,
      elapsed: start.elapsed(),
   });

   // 5. SADD (per-client sets)
   store.flush();
   let start = Instant::now();
   std::thread::scope(|scope| {
      for w in 0..workers {
         let c_start = (w * clients_per_worker).min(clients);
         let c_end = ((w + 1) * clients_per_worker).min(clients);
         let store = Arc::clone(store);
         scope.spawn(move || {
            let mut member_keys = [FastKey::new(b"", 0); 512];
            for c in c_start..c_end {
               let k = FastKey::new(b"s:", c);
               for sent in (0..mix_ops_client).step_by(pipe_size) {
                  let batch = pipe_size.min(mix_ops_client - sent);
                  for j in 0..batch {
                     member_keys[j] = FastKey::new(b"", sent + j);
                  }
                  let mut member_strs = [""; 512];
                  for j in 0..batch {
                     member_strs[j] = member_keys[j].as_str();
                  }
                  let _ = store.sadd(k.as_str(), &member_strs[..batch]);
               }
            }
         });
      }
   });
   results.push(BenchResult {
      label: "SADD (per-client sets)",
      ops: total_mix_ops,
      elapsed: start.elapsed(),
   });

   // 6. ZADD (per-client zsets)
   store.flush();
   let start = Instant::now();
   std::thread::scope(|scope| {
      for w in 0..workers {
         let c_start = (w * clients_per_worker).min(clients);
         let c_end = ((w + 1) * clients_per_worker).min(clients);
         let store = Arc::clone(store);
         scope.spawn(move || {
            let mut member_keys = [FastKey::new(b"", 0); 512];
            for c in c_start..c_end {
               let k = FastKey::new(b"lb:", c);
               for sent in (0..mix_ops_client).step_by(pipe_size) {
                  let batch = pipe_size.min(mix_ops_client - sent);
                  for j in 0..batch {
                     member_keys[j] = FastKey::new(b"", sent + j);
                  }
                  let mut zadd_items = [(0.0f64, ""); 512];
                  for j in 0..batch {
                     zadd_items[j] = ((sent + j) as f64, member_keys[j].as_str());
                  }
                  let _ = store.zadd(k.as_str(), &zadd_items[..batch], ZAddOptions::default());
               }
            }
         });
      }
   });
   results.push(BenchResult {
      label: "ZADD (per-client zsets)",
      ops: total_mix_ops,
      elapsed: start.elapsed(),
   });

   // 7. JSON.SET (documents)
   store.flush();
   let json_val = r#"{"name":"test","score":100}"#;
   let start = Instant::now();
   std::thread::scope(|scope| {
      for w in 0..workers {
         let c_start = (w * clients_per_worker).min(clients);
         let c_end = ((w + 1) * clients_per_worker).min(clients);
         let store = Arc::clone(store);
         scope.spawn(move || {
            let _pin = customhash::pin();
            for c in c_start..c_end {
               let base = c * mix_ops_client;
               for j in 0..mix_ops_client {
                  let k = FastKey::new(b"json:", base + j);
                  let _ = store.json_set(k.as_str(), "$", json_val, false, false);
               }
            }
         });
      }
   });
   results.push(BenchResult {
      label: "JSON.SET (documents)",
      ops: total_mix_ops,
      elapsed: start.elapsed(),
   });

   // 8. JSON.GET (documents)
   let start = Instant::now();
   std::thread::scope(|scope| {
      for w in 0..workers {
         let c_start = (w * clients_per_worker).min(clients);
         let c_end = ((w + 1) * clients_per_worker).min(clients);
         let store = Arc::clone(store);
         scope.spawn(move || {
            let mut out = Vec::with_capacity(64);
            let _pin = customhash::pin();
            for c in c_start..c_end {
               let base = c * mix_ops_client;
               for j in 0..mix_ops_client {
                  let k = FastKey::new(b"json:", base + j);
                  out.clear();
                  let _ = store.json_get_to_buf(k.as_str(), &["$"], &mut out);
               }
            }
         });
      }
   });
   results.push(BenchResult {
      label: "JSON.GET (documents)",
      ops: total_mix_ops,
      elapsed: start.elapsed(),
   });

   // 9. SET+EXPIRE (cache TTL)
   store.flush();
   let exp = expiry_from_secs(60).unwrap_or(0);
   let start = Instant::now();
   std::thread::scope(|scope| {
      for w in 0..workers {
         let c_start = (w * clients_per_worker).min(clients);
         let c_end = ((w + 1) * clients_per_worker).min(clients);
         let store = Arc::clone(store);
         scope.spawn(move || {
            let _pin = customhash::pin();
            for c in c_start..c_end {
               let base = c * mix_ops_client;
               for j in 0..mix_ops_client {
                  let k = FastKey::new(b"expire:", base + j);
                  store.set_string(k.as_str(), "val", exp);
               }
            }
         });
      }
   });
   results.push(BenchResult {
      label: "SET+EXPIRE (cache TTL)",
      ops: total_mix_ops,
      elapsed: start.elapsed(),
   });

   // 10. Hot Key (1 key, contention): Concurrent multi-core contention on 1 key.
   store.flush();
   let hotkey_ops_client = 100_000usize;
   let total_hotkey_ops = (clients * hotkey_ops_client) as i64;
   let hot_ops_per_worker = total_hotkey_ops as usize / workers;
   store.set_string("hot", "0", 0);
   let start = Instant::now();
   std::thread::scope(|scope| {
      for _w in 0..workers {
         let store = Arc::clone(store);
         scope.spawn(move || {
            let mut out = Vec::with_capacity(64);
            let _pin = customhash::pin();
            for sent in (0..hot_ops_per_worker).step_by(pipe_size) {
               let batch = pipe_size.min(hot_ops_per_worker - sent);
               let gets = batch / 2;
               store.set_string("hot", "val", 0);
               for _ in 0..gets {
                  out.clear();
                  let _ = store.get_to_buf("hot", &mut out);
               }
            }
         });
      }
   });
   results.push(BenchResult {
      label: "Hot Key (1 key, contention)",
      ops: total_hotkey_ops,
      elapsed: start.elapsed(),
   });

   // 11. Producer/Consumer (50+50): Half producers, half consumers in parallel.
   store.flush();
   let half = (clients / 2).max(1);
   let queue_ops = 20_000usize;
   let total_queue_ops = (half * queue_ops * 2) as i64;
   let prod_threads = (workers / 2).max(1);
   let cons_threads = (workers - prod_threads).max(1);
   let prod_ops = (half * queue_ops) / prod_threads;
   let cons_ops = (half * queue_ops) / cons_threads;
   let batch_size = 500usize;
   let payload_batch: Vec<&str> = vec!["payload"; batch_size];
   let start = Instant::now();
   std::thread::scope(|scope| {
      for _ in 0..prod_threads {
         let s = Arc::clone(store);
         let payload_batch = &payload_batch;
         scope.spawn(move || {
            let _pin = customhash::pin();
            for sent in (0..prod_ops).step_by(batch_size) {
               let b = batch_size.min(prod_ops - sent);
               let _ = s.lpush("wq", &payload_batch[..b]);
            }
         });
      }
      for _ in 0..cons_threads {
         let s = Arc::clone(store);
         scope.spawn(move || {
            let mut out = Vec::with_capacity(1024);
            for sent in (0..cons_ops).step_by(batch_size) {
               let b = batch_size.min(cons_ops - sent);
               out.clear();
               let _ = s.pop_many_to_buf("wq", b, true, &mut out);
            }
         });
      }
   });
   results.push(BenchResult {
      label: "Producer/Consumer (50+50)",
      ops: total_queue_ops,
      elapsed: start.elapsed(),
   });

   // 12. XADD (streams)
   store.flush();
   let start = Instant::now();
   std::thread::scope(|scope| {
      for w in 0..workers {
         let c_start = (w * clients_per_worker).min(clients);
         let c_end = ((w + 1) * clients_per_worker).min(clients);
         let store = Arc::clone(store);
         scope.spawn(move || {
            for c in c_start..c_end {
               let k = FastKey::new(b"stream:", c);
               for sent in (0..mix_ops_client).step_by(pipe_size) {
                  let batch = pipe_size.min(mix_ops_client - sent);
                  for j in 0..batch {
                     let seq = FastKey::new(b"", sent + j);
                     let _ = store.xadd(k.as_str(), "*", &[("f", seq.as_str())], None, false);
                  }
               }
            }
         });
      }
   });
   results.push(BenchResult {
      label: "XADD (streams)",
      ops: total_mix_ops,
      elapsed: start.elapsed(),
   });

   // 13. SETBIT/BITCOUNT (bitmap)
   store.flush();
   let start = Instant::now();
   std::thread::scope(|scope| {
      for w in 0..workers {
         let c_start = (w * clients_per_worker).min(clients);
         let c_end = ((w + 1) * clients_per_worker).min(clients);
         let store = Arc::clone(store);
         scope.spawn(move || {
            for c in c_start..c_end {
               let k = FastKey::new(b"bmap:", c);
               for sent in (0..mix_ops_client).step_by(pipe_size) {
                  let batch = pipe_size.min(mix_ops_client - sent);
                  for j in 0..batch {
                     let offset = (sent + j) as u64;
                     let _ = store.setbit(k.as_str(), offset, true);
                  }
               }
               let _ = store.bitcount(k.as_str(), 0, -1, false);
            }
         });
      }
   });
   results.push(BenchResult {
      label: "SETBIT/BITCOUNT (bitmap)",
      ops: total_mix_ops,
      elapsed: start.elapsed(),
   });

   // 14. PFADD/PFCOUNT (HyperLogLog)
   store.flush();
   let start = Instant::now();
   std::thread::scope(|scope| {
      for w in 0..workers {
         let c_start = (w * clients_per_worker).min(clients);
         let c_end = ((w + 1) * clients_per_worker).min(clients);
         let store = Arc::clone(store);
         scope.spawn(move || {
            let mut elem_keys = [FastKey::new(b"elem:", 0); 512];
            for c in c_start..c_end {
               let k = FastKey::new(b"hll:", c);
               for sent in (0..mix_ops_client).step_by(pipe_size) {
                  let batch = pipe_size.min(mix_ops_client - sent);
                  for j in 0..batch {
                     elem_keys[j] = FastKey::new(b"elem:", sent + j);
                  }
                  let mut elem_strs = [""; 512];
                  for j in 0..batch {
                     elem_strs[j] = elem_keys[j].as_str();
                  }
                  let _ = store.pfadd(k.as_str(), &elem_strs[..batch]);
               }
               let _ = store.pfcount(&[k.as_str()]);
            }
         });
      }
   });
   results.push(BenchResult {
      label: "PFADD/PFCOUNT (HyperLogLog)",
      ops: total_mix_ops,
      elapsed: start.elapsed(),
   });

   // 15. GEOADD/GEODIST (geospatial)
   store.flush();
   let start = Instant::now();
   std::thread::scope(|scope| {
      for w in 0..workers {
         let c_start = (w * clients_per_worker).min(clients);
         let c_end = ((w + 1) * clients_per_worker).min(clients);
         let store = Arc::clone(store);
         scope.spawn(move || {
            let mut geo_keys = [FastKey::new(b"", 0); 512];
            for c in c_start..c_end {
               let k = FastKey::new(b"geo:", c);
               for sent in (0..mix_ops_client).step_by(pipe_size) {
                  let batch = pipe_size.min(mix_ops_client - sent);
                  for j in 0..batch {
                     geo_keys[j] = FastKey::new(b"", sent + j);
                  }
                  let mut geo_items = [(0.0f64, 0.0f64, ""); 512];
                  for j in 0..batch {
                     let idx = sent + j;
                     let lon = (idx % 360) as f64 - 180.0;
                     let lat = (idx % 170) as f64 - 85.0;
                     geo_items[j] = (lon, lat, geo_keys[j].as_str());
                  }
                  let _ = store.geoadd(k.as_str(), &geo_items[..batch], false, false, false);
               }
               let _ = store.geodist(k.as_str(), "0", "1", GeoUnit::Km);
            }
         });
      }
   });
   results.push(BenchResult {
      label: "GEOADD/GEODIST (geospatial)",
      ops: total_mix_ops,
      elapsed: start.elapsed(),
   });
}

// --- VECTOR SEARCH BENCHMARK ---
fn run_vector(store: &Arc<Store>, workers: usize, results: &mut Vec<BenchResult>) {
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

   let load_threads = std::env::var("FYRODB_SEARCH_THREADS")
      .or_else(|_| std::env::var("FYRODB_WORKERS"))
      .ok()
      .and_then(|v| v.parse::<usize>().ok())
      .unwrap_or(workers)
      .clamp(1, workers);
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
               const WORDS: &[&str] = &[
                  "alpha",
                  "bravo",
                  "charlie",
                  "delta",
                  "echo",
                  "foxtrot",
                  "golf",
                  "hotel",
                  "india",
                  "juliet",
                  "kilo",
                  "lima",
                  "mike",
                  "november",
                  "oscar",
                  "papa",
                  "quebec",
                  "romeo",
                  "sierra",
                  "tango",
                  "uniform",
                  "victor",
                  "whiskey",
                  "xray",
                  "yankee",
                  "zulu",
                  "amber",
                  "basalt",
                  "cobalt",
                  "dune",
                  "ember",
                  "flint",
                  "granite",
                  "harbor",
                  "iris",
                  "jade",
                  "kelp",
                  "lunar",
                  "moss",
                  "nectar",
                  "onyx",
                  "prism",
                  "quartz",
                  "reef",
                  "sage",
                  "tundra",
                  "umber",
                  "vellum",
                  "willow",
                  "xenon",
                  "yarrow",
                  "zephyr",
                  "anchor",
                  "beacon",
                  "cinder",
                  "drift",
                  "edelweiss",
                  "fathom",
                  "gable",
                  "helix",
                  "ingot",
                  "jasper",
                  "kelvin",
                  "lumen",
               ];
               let key = FastKey::new(b"vec:", id);
               let tag = FastKey::new(b"tag_", id % 10);
               let num = FastKey::new(b"", id % 100);
               let title = WORDS[id % WORDS.len()];
               let mut title_buf = [0u8; 32];
               title_buf[..4].copy_from_slice(b"doc ");
               title_buf[4..4 + title.len()].copy_from_slice(title.as_bytes());
               let title_len = 4 + title.len();
               let _ = store.hset_bytes(
                  key.as_str(),
                  &[
                     ("v", &vbytes),
                     ("cat", tag.as_bytes()),
                     ("num", num.as_bytes()),
                     ("title", &title_buf[..title_len]),
                  ],
               );
            }
         });
      }
   });
   results.push(BenchResult {
      label: "Vector HSET load",
      ops: vec_docs as i64,
      elapsed: start.elapsed(),
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
   results.push(BenchResult {
      label: "Vector KNN-10",
      ops: knn_queries as i64,
      elapsed: start.elapsed(),
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
   results.push(BenchResult {
      label: "Vector KNN-10 +tag filter",
      ops: tag_queries as i64,
      elapsed: start.elapsed(),
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
   results.push(BenchResult {
      label: "Filter Tag+Num",
      ops: filter_queries as i64,
      elapsed: start.elapsed(),
   });

   // Text term
   let text_queries = 1_000usize;
   let start = Instant::now();
   let parts_text = [
      "FT.SEARCH",
      index_name,
      "@title:alpha",
      "LIMIT",
      "0",
      "10",
      "NOCONTENT",
   ];
   for _ in 0..text_queries {
      out.clear();
      search_dispatch(&parts_text, &[], store, &mut out);
   }
   results.push(BenchResult {
      label: "Text term",
      ops: text_queries as i64,
      elapsed: start.elapsed(),
   });

   // Text + Tag
   let start = Instant::now();
   let parts_text_tag = [
      "FT.SEARCH",
      index_name,
      "@title:alpha @cat:{tag_0}",
      "LIMIT",
      "0",
      "10",
      "NOCONTENT",
   ];
   for _ in 0..text_queries {
      out.clear();
      search_dispatch(&parts_text_tag, &[], store, &mut out);
   }
   results.push(BenchResult {
      label: "Text + Tag",
      ops: text_queries as i64,
      elapsed: start.elapsed(),
   });

   // Text negation
   let start = Instant::now();
   let parts_text_neg = [
      "FT.SEARCH",
      index_name,
      "-@title:alpha",
      "LIMIT",
      "0",
      "10",
      "NOCONTENT",
   ];
   for _ in 0..text_queries {
      out.clear();
      search_dispatch(&parts_text_neg, &[], store, &mut out);
   }
   results.push(BenchResult {
      label: "Text negation",
      ops: text_queries as i64,
      elapsed: start.elapsed(),
   });

   // Vector KNN-10 concurrent clients (capped by workers)
   let knn_clients = workers.max(1);
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
   results.push(BenchResult {
      label: Box::leak(format!("Vector KNN-10 ({knn_clients} clients)").into_boxed_str()),
      ops: knn_queries as i64,
      elapsed: start.elapsed(),
   });

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
   results.push(BenchResult {
      label: "Vector HNSW KNN-10",
      ops: knn_queries as i64,
      elapsed: start.elapsed(),
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
   results.push(BenchResult {
      label: "Vector HNSW M=32 KNN-10",
      ops: knn_queries as i64,
      elapsed: start.elapsed(),
   });
}
