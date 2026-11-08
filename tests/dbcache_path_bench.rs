use std::time::Instant;
use std::sync::Arc;
use fyro_dbcache::EncodedResult;
use fyro_dbcache::query::{parse_sql, make_cache_key_inline};
use fyro_dbcache::{ColumnInfo, DbResultSet, DbRow, DbValue};

fn make_sample_result() -> DbResultSet {
   DbResultSet {
      columns: vec![
         ColumnInfo { name: "id".to_string(), type_oid: 23 },
         ColumnInfo { name: "username".to_string(), type_oid: 25 },
         ColumnInfo { name: "email".to_string(), type_oid: 25 },
      ],
      rows: vec![
         DbRow {
            values: vec![
               DbValue::Int(42),
               DbValue::Text("alice".to_string()),
               DbValue::Text("alice@example.com".to_string()),
            ],
         },
      ],
      rows_affected: 1,
   }
}

#[test]
#[allow(clippy::const_is_empty)]
fn bench_dbcache_proxy_path_optimizations() {
   println!("\n==========================================================================");
   println!("       ⚡ FYRODB DB-CACHE PROXY PATH BENCHMARK: BEFORE VS AFTER ⚡");
   println!("==========================================================================\n");

   // -------------------------------------------------------------------------
   // BENCHMARK 1: Alternating Queries (1-Slot Cache vs 8-Slot Cache)
   // -------------------------------------------------------------------------
   {
      const ITERATIONS: usize = 200_000;
      let sql_a = "SELECT id, username FROM users WHERE id = 42";
      let sql_b = "SELECT id, product_name, price FROM orders WHERE id = 100";

      let parsed_a = parse_sql(sql_a);
      let parsed_b = parse_sql(sql_b);

      let fp_a = parsed_a.fingerprint;
      let fp_b = parsed_b.fingerprint;

      let res_a = make_sample_result();
      let encoded_a = Arc::new(EncodedResult::from_result_set(&res_a, 60_000));

      let res_b = make_sample_result();
      let encoded_b = Arc::new(EncodedResult::from_result_set(&res_b, 60_000));

      // Before: Single hot query slot (hot_sql_fp, hot_encoded)
      // Alternating between A and B causes 100% cache thrashing
      let mut out_before = Vec::with_capacity(1024);
      let start_before = Instant::now();
      let mut slot_fp = 0u64;
      let mut slot_encoded: Option<Arc<EncodedResult>> = None;
      let mut before_hits = 0usize;
      let mut _before_misses = 0usize;

      for i in 0..ITERATIONS {
         let (cur_sql, cur_fp, cur_encoded) = if (i & 1) == 0 {
            (sql_a, fp_a, &encoded_a)
         } else {
            (sql_b, fp_b, &encoded_b)
         };

         if slot_fp == cur_fp && let Some(entry) = &slot_encoded {
            out_before.clear();
            entry.write_simple_response(&mut out_before);
            before_hits += 1;
         } else {
            // Miss: must analyze and reload from storage
            let _p = parse_sql(cur_sql);
            slot_fp = cur_fp;
            slot_encoded = Some(Arc::clone(cur_encoded));
            out_before.clear();
            cur_encoded.write_simple_response(&mut out_before);
            _before_misses += 1;
         }
      }
      let elapsed_before = start_before.elapsed();

      // After: 8-Slot direct-mapped hot query cache
      let mut out_after = Vec::with_capacity(1024);
      let start_after = Instant::now();
      #[derive(Clone)]
      struct HotQuerySlot {
         fp: u64,
         encoded: Arc<EncodedResult>,
      }
      let mut hot_slots: [Option<HotQuerySlot>; 8] = [None, None, None, None, None, None, None, None];
      let mut after_hits = 0usize;
      let mut _after_misses = 0usize;

      for i in 0..ITERATIONS {
         let (cur_sql, cur_fp, cur_encoded) = if (i & 1) == 0 {
            (sql_a, fp_a, &encoded_a)
         } else {
            (sql_b, fp_b, &encoded_b)
         };

         let slot_idx = (cur_fp as usize) & 7;
         if let Some(slot) = &hot_slots[slot_idx] && slot.fp == cur_fp {
            out_after.clear();
            slot.encoded.write_simple_response(&mut out_after);
            after_hits += 1;
         } else {
            let _p = parse_sql(cur_sql);
            hot_slots[slot_idx] = Some(HotQuerySlot {
               fp: cur_fp,
               encoded: Arc::clone(cur_encoded),
            });
            out_after.clear();
            cur_encoded.write_simple_response(&mut out_after);
            _after_misses += 1;
         }
      }
      let elapsed_after = start_after.elapsed();

      println!("Benchmark 1: Alternating Queries (200k iterations)");
      println!("  • Before (1-slot hot cache):  {:.2?} ({:.2}M QPS, Hit rate: {:.1}%)",
         elapsed_before,
         (ITERATIONS as f64 / elapsed_before.as_secs_f64()) / 1_000_000.0,
         (before_hits as f64 / ITERATIONS as f64) * 100.0);
      println!("  • After  (8-slot hot cache):  {:.2?} ({:.2}M QPS, Hit rate: {:.1}%)",
         elapsed_after,
         (ITERATIONS as f64 / elapsed_after.as_secs_f64()) / 1_000_000.0,
         (after_hits as f64 / ITERATIONS as f64) * 100.0);
      let speedup = elapsed_before.as_secs_f64() / elapsed_after.as_secs_f64();
      println!("  ==> SPEEDUP: {:.2}x faster! (Latency dropped from {:.2} ns/op to {:.2} ns/op)\n",
         speedup,
         elapsed_before.as_nanos() as f64 / ITERATIONS as f64,
         elapsed_after.as_nanos() as f64 / ITERATIONS as f64);
   }

   // -------------------------------------------------------------------------
   // BENCHMARK 2: Unnamed Statement & Portal Management (Extended Protocol)
   // -------------------------------------------------------------------------
   {
      const ITERATIONS: usize = 1_000_000;
      use std::collections::HashMap;

      struct Stmt {
         #[allow(dead_code)]
         sql: String,
         fp: u64,
      }
      struct Portal {
         #[allow(dead_code)]
         stmt: String,
         fp: u64,
      }

      // Before: HashMap with String allocation for unnamed statement ("") and unnamed portal ("")
      let mut stmts_map: HashMap<String, Stmt> = HashMap::new();
      let mut portals_map: HashMap<String, Portal> = HashMap::new();
      let start_before = Instant::now();
      for i in 0..ITERATIONS {
         let name = "";
         let portal_name = "";
         // Parse:
         stmts_map.insert(name.to_string(), Stmt { sql: "SELECT 1".to_string(), fp: i as u64 });
         // Bind:
         let s = stmts_map.get(name).unwrap();
         portals_map.insert(portal_name.to_string(), Portal { stmt: name.to_string(), fp: s.fp });
         // Execute:
         let p = portals_map.get_mut(portal_name).unwrap();
         std::hint::black_box(p.fp);
      }
      let elapsed_before = start_before.elapsed();

      // After: Direct unnamed_stmt and unnamed_portal Option slots (0 String allocations, 0 hashing)
      let mut unnamed_stmt: Option<Stmt> = None;
      let mut unnamed_portal: Option<Portal> = None;
      let start_after = Instant::now();
      for i in 0..ITERATIONS {
         let name = "";
         let portal_name = "";
         // Parse:
         if name.is_empty() {
            unnamed_stmt = Some(Stmt { sql: "SELECT 1".to_string(), fp: i as u64 });
         } else {
            stmts_map.insert(name.to_string(), Stmt { sql: "SELECT 1".to_string(), fp: i as u64 });
         }
         // Bind:
         let s = if name.is_empty() { unnamed_stmt.as_ref().unwrap() } else { stmts_map.get(name).unwrap() };
         if portal_name.is_empty() {
            unnamed_portal = Some(Portal { stmt: name.to_string(), fp: s.fp });
         } else {
            portals_map.insert(portal_name.to_string(), Portal { stmt: name.to_string(), fp: s.fp });
         }
         // Execute:
         let p = if portal_name.is_empty() { unnamed_portal.as_mut().unwrap() } else { portals_map.get_mut(portal_name).unwrap() };
         std::hint::black_box(p.fp);
      }
      let elapsed_after = start_after.elapsed();

      println!("Benchmark 2: Unnamed Statement/Portal Extended Protocol (1M cycles)");
      println!("  • Before (HashMap<String, Stmt> + allocations): {:.2?} ({:.2}M ops/s)",
         elapsed_before,
         (ITERATIONS as f64 / elapsed_before.as_secs_f64()) / 1_000_000.0);
      println!("  • After  (Option<Stmt> O(1) zero-alloc slots):   {:.2?} ({:.2}M ops/s)",
         elapsed_after,
         (ITERATIONS as f64 / elapsed_after.as_secs_f64()) / 1_000_000.0);
      let speedup = elapsed_before.as_secs_f64() / elapsed_after.as_secs_f64();
      println!("  ==> SPEEDUP: {:.2}x faster! (Latency dropped from {:.2} ns/op to {:.2} ns/op)\n",
         speedup,
         elapsed_before.as_nanos() as f64 / ITERATIONS as f64,
         elapsed_after.as_nanos() as f64 / ITERATIONS as f64);
   }

   // -------------------------------------------------------------------------
   // BENCHMARK 3: Execute Response Wire Serialization (write_execute_response)
   // -------------------------------------------------------------------------
   {
      const ITERATIONS: usize = 5_000_000;
      let res = make_sample_result();
      let encoded = EncodedResult::from_result_set(&res, 60_000);

      // Old: slice splitting, calculating lengths, and two separate extend_from_slice calls
      let mut out_old = Vec::with_capacity(512);
      let start_old = Instant::now();
      for _ in 0..ITERATIONS {
         out_old.clear();
         let payload = encoded.data_rows().len() + encoded.command_complete().len();
         out_old.reserve(payload);
         out_old.extend_from_slice(encoded.data_rows());
         out_old.extend_from_slice(encoded.command_complete());
         std::hint::black_box(out_old.as_slice());
      }
      let elapsed_old = start_old.elapsed();

      // New: direct contiguous slice write_execute_response
      let mut out_new = Vec::with_capacity(512);
      let start_new = Instant::now();
      for _ in 0..ITERATIONS {
         out_new.clear();
         encoded.write_execute_response(&mut out_new);
         std::hint::black_box(out_new.as_slice());
      }
      let elapsed_new = start_new.elapsed();

      println!("Benchmark 3: Execute Response Wire Serialization (5M iterations)");
      println!("  • Before (calculate lens + 2 slice calls): {:.2?} ({:.2}M ops/s)",
         elapsed_old,
         (ITERATIONS as f64 / elapsed_old.as_secs_f64()) / 1_000_000.0);
      println!("  • After  (single contiguous slice write):  {:.2?} ({:.2}M ops/s)",
         elapsed_new,
         (ITERATIONS as f64 / elapsed_new.as_secs_f64()) / 1_000_000.0);
      let speedup = elapsed_old.as_secs_f64() / elapsed_new.as_secs_f64();
      println!("  ==> SPEEDUP: {:.2}x faster! (Latency dropped from {:.2} ns/op to {:.2} ns/op)\n",
         speedup,
         elapsed_old.as_nanos() as f64 / ITERATIONS as f64,
         elapsed_new.as_nanos() as f64 / ITERATIONS as f64);
   }

   // -------------------------------------------------------------------------
   // BENCHMARK 4: Command Complete Protocol Frame Generation
   // -------------------------------------------------------------------------
   {
      const ITERATIONS: usize = 5_000_000;
      let tag = "BEGIN";

      // Before: allocate temporary Vec<u8> then copy
      let mut out_before = Vec::with_capacity(512);
      let start_before = Instant::now();
      for _ in 0..ITERATIONS {
         out_before.clear();
         let mut body = Vec::with_capacity(tag.len() + 1);
         body.extend_from_slice(tag.as_bytes());
         body.push(0);
         let mut frame = Vec::with_capacity(body.len() + 5);
         fyro_dbcache::protocol::put_message(&mut frame, b'C', &body);
         out_before.extend_from_slice(&frame);
         std::hint::black_box(out_before.as_slice());
      }
      let elapsed_before = start_before.elapsed();

      // After: zero-allocation direct put_command_complete
      let mut out_after = Vec::with_capacity(512);
      let start_after = Instant::now();
      for _ in 0..ITERATIONS {
         out_after.clear();
         fyro_dbcache::protocol::put_command_complete(&mut out_after, tag);
         std::hint::black_box(out_after.as_slice());
      }
      let elapsed_after = start_after.elapsed();

      println!("Benchmark 4: Command Complete Framing (5M iterations)");
      println!("  • Before (temporary Vec allocations):  {:.2?} ({:.2}M ops/s)",
         elapsed_before,
         (ITERATIONS as f64 / elapsed_before.as_secs_f64()) / 1_000_000.0);
      println!("  • After  (zero-alloc direct buffer write): {:.2?} ({:.2}M ops/s)",
         elapsed_after,
         (ITERATIONS as f64 / elapsed_after.as_secs_f64()) / 1_000_000.0);
      let speedup = elapsed_before.as_secs_f64() / elapsed_after.as_secs_f64();
      println!("  ==> SPEEDUP: {:.2}x faster! (Latency dropped from {:.2} ns/op to {:.2} ns/op)\n",
         speedup,
         elapsed_before.as_nanos() as f64 / ITERATIONS as f64,
         elapsed_after.as_nanos() as f64 / ITERATIONS as f64);
   }

   // -------------------------------------------------------------------------
   // BENCHMARK 5: Cache Key Generation (make_cache_key_inline)
   // -------------------------------------------------------------------------
   {
      const ITERATIONS: usize = 5_000_000;
      let mut buf = [0u8; 512];
      let prefix = "db:";
      let table = "users";
      let fp = 0xDEADBEEFCAFE1234u64;

      let start = Instant::now();
      for _ in 0..ITERATIONS {
         let key = make_cache_key_inline(prefix, table, fp, &mut buf);
         std::hint::black_box(key);
      }
      let elapsed = start.elapsed();

      println!("Benchmark 5: Inline Cache Key Generation (5M iterations)");
      println!("  • Optimized make_cache_key_inline: {:.2?} ({:.2}M ops/s, {:.2} ns/op)",
         elapsed,
         (ITERATIONS as f64 / elapsed.as_secs_f64()) / 1_000_000.0,
         elapsed.as_nanos() as f64 / ITERATIONS as f64);
   }
   println!("==========================================================================\n");
}
