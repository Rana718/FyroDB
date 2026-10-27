//! script_bench — FyroDB Lua scripting benchmark suite.
//! Measures single-thread and multi-thread throughput for EVAL and EVALSHA.
//!
//! Run with:
//!   cargo bench --bench script_bench

use fyro_db::commands;
use fyro_db::storage::store::Store;
use rust_zmalloc::Zmalloc;
use std::sync::Arc;
use std::time::Instant;

#[global_allocator]
static GLOBAL: Zmalloc = Zmalloc;

fn run_eval(store: &Arc<Store>, parts: &[&str], out: &mut Vec<u8>) {
   out.clear();
   commands::execute(parts, store, out);
}

fn bench_rate(label: &str, ops: usize, mut f: impl FnMut()) {
   // Warmup
   for _ in 0..1_000 {
      f();
   }
   let start = Instant::now();
   for _ in 0..ops {
      f();
   }
   let elapsed = start.elapsed();
   let rate = (ops as f64) / elapsed.as_secs_f64();
   println!(
      "  {:<45} {:>10.2?}  {:>10.2} kops/sec",
      label,
      elapsed,
      rate / 1_000.0
   );
}

fn main() {
   println!("\n============================================================");
   println!("          ⚡ FYRODB LUA SCRIPTING BENCHMARK ⚡              ");
   println!("============================================================\n");

   let store = Arc::new(Store::new());
   let mut out = Vec::with_capacity(256);

   // Pre-seed some keys for GET / INCR benchmarks
   commands::execute(&["SET", "bench:key", "100"], &store, &mut out);

   // 1. EVAL basic return (measures overhead of EVAL + compilation + return)
   bench_rate("EVAL 'return 42' (with compilation)", 50_000, || {
      run_eval(&store, &["EVAL", "return 42", "0"], &mut out);
   });

   // 2. Pre-load scripts for EVALSHA
   let mut load_out = Vec::new();
   commands::execute(&["SCRIPT", "LOAD", "return 42"], &store, &mut load_out);
   let sha_basic = String::from_utf8(load_out.clone()).unwrap();
   let sha_basic = sha_basic.trim_start_matches("$40\r\n").trim_end_matches("\r\n");

   bench_rate("EVALSHA 'return 42' (precompiled)", 100_000, || {
      run_eval(&store, &["EVALSHA", sha_basic, "0"], &mut out);
   });

   // 3. EVALSHA with KEYS and ARGV
   commands::execute(&["SCRIPT", "LOAD", "return {KEYS[1], ARGV[1]}"], &store, &mut load_out);
   let sha_keys = String::from_utf8(load_out.clone()).unwrap();
   let sha_keys = sha_keys.trim_start_matches("$40\r\n").trim_end_matches("\r\n");

   bench_rate("EVALSHA KEYS[1] + ARGV[1]", 100_000, || {
      run_eval(&store, &["EVALSHA", sha_keys, "1", "bench:key", "arg_val"], &mut out);
   });

   // 4. EVALSHA with redis.call('GET', KEYS[1])
   commands::execute(&["SCRIPT", "LOAD", "return redis.call('GET', KEYS[1])"], &store, &mut load_out);
   let sha_get = String::from_utf8(load_out.clone()).unwrap();
   let sha_get = sha_get.trim_start_matches("$40\r\n").trim_end_matches("\r\n");

   bench_rate("EVALSHA redis.call('GET', key)", 100_000, || {
      run_eval(&store, &["EVALSHA", sha_get, "1", "bench:key"], &mut out);
   });

   // 5. EVALSHA with redis.call('INCR', KEYS[1])
   commands::execute(&["SCRIPT", "LOAD", "return redis.call('INCR', KEYS[1])"], &store, &mut load_out);
   let sha_incr = String::from_utf8(load_out.clone()).unwrap();
   let sha_incr = sha_incr.trim_start_matches("$40\r\n").trim_end_matches("\r\n");

   bench_rate("EVALSHA redis.call('INCR', key)", 100_000, || {
      run_eval(&store, &["EVALSHA", sha_incr, "1", "bench:counter"], &mut out);
   });

   // 6. EVALSHA CAS (Compare-And-Swap) pattern
   let cas_script = r#"
      local current = redis.call('GET', KEYS[1])
      if current == ARGV[1] then
         redis.call('SET', KEYS[1], ARGV[2])
         return 1
      else
         return 0
      end
   "#;
   commands::execute(&["SCRIPT", "LOAD", cas_script], &store, &mut load_out);
   let sha_cas = String::from_utf8(load_out.clone()).unwrap();
   let sha_cas = sha_cas.trim_start_matches("$40\r\n").trim_end_matches("\r\n");

   bench_rate("EVALSHA CAS (GET + compare + SET)", 100_000, || {
      run_eval(&store, &["EVALSHA", sha_cas, "1", "bench:key", "100", "100"], &mut out);
   });

   // 7. Multi-threaded concurrent EVALSHA
   println!("\n── Multi-threaded Concurrent Throughput (4 threads) ───────");
   let threads = 4;
   let ops_per_thread = 50_000;
   let total_ops = threads * ops_per_thread;

   let start = Instant::now();
   let handles: Vec<_> = (0..threads)
      .map(|t_id| {
         let s = Arc::clone(&store);
         let sha = sha_get.to_string();
         std::thread::spawn(move || {
            let mut buf = Vec::with_capacity(256);
            let key = format!("bench:thread:{t_id}");
            commands::execute(&["SET", &key, "initial_value"], &s, &mut buf);
            for _ in 0..ops_per_thread {
               run_eval(&s, &["EVALSHA", &sha, "1", &key], &mut buf);
            }
         })
      })
      .collect();

   for h in handles {
      h.join().unwrap();
   }
   let elapsed = start.elapsed();
   let rate = (total_ops as f64) / elapsed.as_secs_f64();
   println!(
      "  {:<45} {:>10.2?}  {:>10.2} kops/sec",
      "Concurrent EVALSHA GET (4 threads)",
      elapsed,
      rate / 1_000.0
   );
   println!("============================================================\n");
}
