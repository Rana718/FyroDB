use fyro_db::storage::store::Store;
use fyro_dbcache::protocol::{PROTOCOL_VERSION_3, encode_bind, put_message};
use fyro_dbcache::query::{compute_fingerprint_with_params, parse_sql};
use fyro_dbcache::row::{ColumnInfo, DbResultSet, DbRow, DbValue};
use fyro_dbcache::{DbCacheConfig, DbCacheManager, DbProxyMode};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
struct Scale {
   threads: usize,
   direct_ops: usize,
   encoded_ops: usize,
   joins: usize,
   tcp_clients: usize,
   tcp_queries: usize,
}

const FULL: Scale = Scale {
   threads: 4,
   direct_ops: 500_000,
   encoded_ops: 2_000_000,
   joins: 200_000,
   tcp_clients: 32,
   tcp_queries: 50_000,
};

// Small enough to run inside the regular benchmark tasks as a sanity check.
const SMOKE: Scale = Scale {
   threads: 2,
   direct_ops: 10_000,
   encoded_ops: 20_000,
   joins: 2_000,
   tcp_clients: 2,
   tcp_queries: 500,
};

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
   let smoke = std::env::args()
      .skip(1)
      .any(|arg| arg == "--smoke" || arg == "-s");
   let scale = if smoke { SMOKE } else { FULL };

   println!("============================================================");
   println!("           FyroDB PostgreSQL Path Benchmark                 ");
   if smoke {
      println!("           (smoke mode: reduced iterations)                 ");
   }
   println!("============================================================");

   let mut results = Vec::with_capacity(5);
   bench_in_memory_direct(scale, &mut results);
   bench_encoded_multithread(scale, &mut results);
   bench_in_memory_complex_joins(scale, &mut results);
   bench_extended_protocol_tcp(scale, &mut results);
   bench_tcp_wire_proxy(scale, &mut results);

   print_summary_table(&results);
}

// Microbenchmarks direct in-memory CustomMap lookup and JSON serialization.
fn bench_in_memory_direct(scale: Scale, results: &mut Vec<BenchResult>) {
   let store = Arc::new(Store::new());
   let manager = Arc::new(DbCacheManager::new(store.clone()));

   let sql = "SELECT id, username, email FROM users WHERE id = 42;";
   let parsed = parse_sql(sql);
   let rs = sample_result_set();

   manager.store_cached_query("db:", &parsed.tables, parsed.fingerprint, &rs, 60_000);

   let threads = scale.threads;
   let ops_per_thread = scale.direct_ops;
   let total_ops = threads * ops_per_thread;

   let start = Instant::now();
   let mut handles = Vec::new();

   for _ in 0..threads {
      let mgr = Arc::clone(&manager);
      let p = parsed.clone();
      handles.push(std::thread::spawn(move || {
         for _ in 0..ops_per_thread {
            let res = mgr.get_cached_query("db:", "users", p.fingerprint);
            std::hint::black_box(res);
         }
      }));
   }

   for h in handles {
      h.join().unwrap();
   }

   results.push(BenchResult {
      label: "In-memory cache lookup",
      ops: total_ops as i64,
      elapsed: start.elapsed(),
   });
}

// Microbenchmarks the proxy hot path: pre-encoded wire response fetched from the
// sharded cache. SQL analysis is done once per thread (simulating the parse cache
// that the real session uses), so each iteration measures only the encoded lookup.
fn bench_encoded_multithread(scale: Scale, results: &mut Vec<BenchResult>) {
   let store = Arc::new(Store::new());
   let manager = Arc::new(DbCacheManager::new(store.clone()));

   let sql = "SELECT id, username, email FROM users WHERE id = 42;";
   let parsed = parse_sql(sql);
   let rs = sample_result_set();
   manager.store_cached_query("db:", &parsed.tables, parsed.fingerprint, &rs, 60_000);

   let threads = scale.threads;
   let ops_per_thread = scale.encoded_ops;
   let total_ops = threads * ops_per_thread;

   let start = Instant::now();
   let mut handles = Vec::new();

   for _ in 0..threads {
      let mgr = Arc::clone(&manager);
      let query = sql.to_string();
      handles.push(std::thread::spawn(move || {
         // analyze() once per thread — mirrors the ParsedQueryCache hit path.
         let p = mgr.analyze(&query);
         for _ in 0..ops_per_thread {
            let res = mgr.get_encoded("db:", "users", p.fingerprint);
            std::hint::black_box(res);
         }
      }));
   }

   for h in handles {
      h.join().unwrap();
   }

   results.push(BenchResult {
      label: "Encoded cache lookup",
      ops: total_ops as i64,
      elapsed: start.elapsed(),
   });
}

// Microbenchmarks complex 3-table JOIN queries and AST parsing.
fn bench_in_memory_complex_joins(scale: Scale, results: &mut Vec<BenchResult>) {
   let store = Arc::new(Store::new());
   let manager = Arc::new(DbCacheManager::new(store.clone()));

   let join_sql = "SELECT u.name, o.total, p.title \
                   FROM users u \
                   JOIN orders o ON u.id = o.user_id \
                   JOIN products p ON o.product_id = p.id \
                   WHERE u.id = 10;";
   let parsed = parse_sql(join_sql);
   let rs = sample_result_set();

   manager.store_cached_query("db:", &parsed.tables, parsed.fingerprint, &rs, 60_000);

   let count = scale.joins;
   let start = Instant::now();

   for _ in 0..count {
      // analyze() routes through ParsedQueryCache — parse_sql is called only
      // on the first iteration; subsequent iterations are a sharded RwLock read.
      let p = manager.analyze(join_sql);
      let res = manager.get_cached_query("db:", "users", p.fingerprint);
      std::hint::black_box(res);
   }

   results.push(BenchResult {
      label: "JOIN parse + cache lookup",
      ops: count as i64,
      elapsed: start.elapsed(),
   });
}

// Benchmarks the extended query protocol (Parse once, then Bind/Execute/Sync)
// served entirely from the wire cache, with pipelined batches of requests.
fn bench_extended_protocol_tcp(scale: Scale, results: &mut Vec<BenchResult>) {
   let store = Arc::new(Store::new());
   let manager = Arc::new(DbCacheManager::new(store.clone()));

   let port = 15434;
   let config = DbCacheConfig {
      enabled: true,
      mode: DbProxyMode::Dual,
      upstream_addr: None,
      listen_addr: format!("127.0.0.1:{port}"),
      default_ttl_ms: 60_000,
      key_prefix: "db:".to_string(),
      flush_on_start: true,
      sync_interval_ms: 0,
      cache_max_keys: 100_000,
      ttl_jitter_pct: 0,
      ..DbCacheConfig::default()
   };

   let sql = "SELECT id, username, email FROM users WHERE id = $1;";
   let parsed = parse_sql(sql);
   let param = Some(b"42".to_vec());
   let fingerprint =
      compute_fingerprint_with_params(&parsed.normalized, std::slice::from_ref(&param), &[]);
   let rs = sample_result_set();
   manager.store_cached_query("db:", &parsed.tables, fingerprint, &rs, 60_000);

   let _handles =
      fyro_dbcache::start_proxy_listener_mio(config, manager.clone()).expect("start proxy");
   std::thread::sleep(Duration::from_millis(50));

   let num_clients = scale.tcp_clients;
   let queries_per_client = scale.tcp_queries;

   let start = Instant::now();
   let completed = Arc::new(AtomicU64::new(0));
   let mut client_handles = Vec::new();

   for _ in 0..num_clients {
      let done = Arc::clone(&completed);
      let query_str = sql.to_string();
      let bind_param = param.clone();
      client_handles.push(std::thread::spawn(move || {
         let (write_stream, mut br) = connect_startup_streams(port);

         // Parse once; proxy caches the parsed statement locally.
         let mut body = Vec::new();
         body.extend_from_slice(b"s1\0");
         body.extend_from_slice(query_str.as_bytes());
         body.push(0);
         body.extend_from_slice(&0i16.to_be_bytes());
         let mut parse_frame = Vec::new();
         put_message(&mut parse_frame, b'P', &body);
         (&write_stream).write_all(&parse_frame).unwrap();

         let bind_frame = encode_bind("p1", "s1", &[], &[bind_param], &[]);
         let mut execute_body = b"p1\0".to_vec();
         execute_body.extend_from_slice(&0i32.to_be_bytes());
         let mut execute_frame = Vec::new();
         put_message(&mut execute_frame, b'E', &execute_body);

         // Pre-build one Bind+Execute+Sync unit and the full PIPE batch buffer.
         const PIPE: usize = 512;
         let unit_len = bind_frame.len() + execute_frame.len() + 5;
         let mut unit = Vec::with_capacity(unit_len);
         unit.extend_from_slice(&bind_frame);
         unit.extend_from_slice(&execute_frame);
         unit.extend_from_slice(&[b'S', 0, 0, 0, 4]);

         let mut full_batch_buf = Vec::with_capacity(PIPE * unit_len);
         for _ in 0..PIPE {
            full_batch_buf.extend_from_slice(&unit);
         }

         let mut sent = 0;
         while sent < queries_per_client {
            let batch = PIPE.min(queries_per_client - sent);
            let send_slice = if batch == PIPE {
               &full_batch_buf
            } else {
               &full_batch_buf[..batch * unit_len]
            };
            (&write_stream).write_all(send_slice).unwrap();
            drain_ready_batches(&mut br, batch);
            done.fetch_add(batch as u64, Ordering::Relaxed);
            sent += batch;
         }
      }));
   }

   for h in client_handles {
      h.join().unwrap();
   }

   let elapsed = start.elapsed();
   let total = completed.load(Ordering::Relaxed);
   results.push(BenchResult {
      label: "Extended protocol TCP",
      ops: total as i64,
      elapsed,
   });
}

// Benchmarks real PostgreSQL TCP wire protocol round-trip through proxy port.
// Uses pipelined batches of PIPE requests per write syscall to maximize throughput.
fn bench_tcp_wire_proxy(scale: Scale, results: &mut Vec<BenchResult>) {
   let store = Arc::new(Store::new());
   let manager = Arc::new(DbCacheManager::new(store.clone()));

   let port = 15433;
   let config = DbCacheConfig {
      enabled: true,
      mode: DbProxyMode::Dual,
      upstream_addr: None,
      listen_addr: format!("127.0.0.1:{port}"),
      default_ttl_ms: 60_000,
      key_prefix: "db:".to_string(),
      flush_on_start: true,
      sync_interval_ms: 0,
      cache_max_keys: 100_000,
      ttl_jitter_pct: 0,
      ..DbCacheConfig::default()
   };

   let sql = "SELECT id, username, email FROM users WHERE id = 42;";
   let parsed = parse_sql(sql);
   let rs = sample_result_set();
   manager.store_cached_query("db:", &parsed.tables, parsed.fingerprint, &rs, 60_000);

   let _handles =
      fyro_dbcache::start_proxy_listener_mio(config, manager.clone()).expect("start proxy");
   std::thread::sleep(Duration::from_millis(50));

   let num_clients = scale.tcp_clients;
   let queries_per_client = scale.tcp_queries;

   let start = Instant::now();
   let completed = Arc::new(AtomicU64::new(0));
   let mut client_handles = Vec::new();

   for _ in 0..num_clients {
      let done = Arc::clone(&completed);
      let query_str = sql.to_string();
      client_handles.push(std::thread::spawn(move || {
         let (write_stream, mut br) = connect_startup_streams(port);

         let q_bytes = query_str.as_bytes();
         let q_len = (q_bytes.len() + 5) as i32;
         let mut q_pkt = Vec::with_capacity(q_bytes.len() + 5);
         q_pkt.push(b'Q');
         q_pkt.extend_from_slice(&q_len.to_be_bytes());
         q_pkt.extend_from_slice(q_bytes);
         q_pkt.push(0);

         // Pre-build the full PIPE batch buffer once.
         const PIPE: usize = 512;
         let mut full_batch_buf = Vec::with_capacity(PIPE * q_pkt.len());
         for _ in 0..PIPE {
            full_batch_buf.extend_from_slice(&q_pkt);
         }

         let mut sent = 0;
         while sent < queries_per_client {
            let batch = PIPE.min(queries_per_client - sent);
            let send_slice = if batch == PIPE {
               &full_batch_buf
            } else {
               &full_batch_buf[..batch * q_pkt.len()]
            };
            (&write_stream).write_all(send_slice).unwrap();
            drain_ready_batches(&mut br, batch);
            done.fetch_add(batch as u64, Ordering::Relaxed);
            sent += batch;
         }
      }));
   }

   for h in client_handles {
      h.join().unwrap();
   }

   let elapsed = start.elapsed();
   let total = completed.load(Ordering::Relaxed);
   results.push(BenchResult {
      label: "Simple query TCP",
      ops: total as i64,
      elapsed,
   });
}

// Opens a connection, performs the PG startup handshake, and returns the raw
// TcpStream plus a buffered reader over a cloned socket. Used by the pipelining
// benchmarks so they can batch writes on the write socket while reading from
// the buffered reader without per-message heap allocations.
fn connect_startup_streams(port: u16) -> (TcpStream, BufReader<TcpStream>) {
   let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to proxy");
   stream.set_nodelay(true).unwrap();
   let read_clone = stream.try_clone().expect("clone stream for read side");

   let mut startup_payload = Vec::new();
   startup_payload.extend_from_slice(&PROTOCOL_VERSION_3.to_be_bytes());
   startup_payload.extend_from_slice(b"user\0postgres\0database\0test\0\0");
   let len = (startup_payload.len() + 4) as i32;
   (&stream).write_all(&len.to_be_bytes()).unwrap();
   (&stream).write_all(&startup_payload).unwrap();

   let mut br = BufReader::with_capacity(256 * 1024, read_clone);
   drain_until_ready(&mut br);
   (stream, br)
}

// Drains backend messages from a buffered reader until ReadyForQuery ('Z'),
// discarding message bodies without heap allocation.
#[inline(always)]
fn drain_ready_batches(r: &mut BufReader<TcpStream>, mut count: usize) {
   while count > 0 {
      let buf = r.fill_buf().unwrap();
      if buf.is_empty() {
         break;
      }
      let mut pos = 0;
      let len = buf.len();
      while pos + 5 <= len && count > 0 {
         let tag = buf[pos];
         let msg_len =
            i32::from_be_bytes([buf[pos + 1], buf[pos + 2], buf[pos + 3], buf[pos + 4]]) as usize;
         let total = 1 + msg_len;
         if pos + total <= len {
            pos += total;
            if tag == b'Z' {
               count -= 1;
            }
         } else {
            break;
         }
      }
      if pos > 0 {
         r.consume(pos);
      } else {
         drain_until_ready(r);
         count -= 1;
      }
   }
}

fn drain_until_ready(r: &mut BufReader<TcpStream>) {
   let mut head = [0u8; 5];
   loop {
      r.read_exact(&mut head).unwrap();
      let body_len =
         (i32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize).saturating_sub(4);
      let mut remaining = body_len;
      while remaining > 0 {
         let buf = r.fill_buf().unwrap();
         if buf.is_empty() {
            break;
         }
         let take = buf.len().min(remaining);
         r.consume(take);
         remaining -= take;
      }
      if head[0] == b'Z' {
         break;
      }
   }
}

fn sample_result_set() -> DbResultSet {
   DbResultSet {
      columns: vec![
         ColumnInfo {
            name: "id".to_string(),
            type_oid: 23,
         },
         ColumnInfo {
            name: "username".to_string(),
            type_oid: 25,
         },
         ColumnInfo {
            name: "email".to_string(),
            type_oid: 25,
         },
      ],
      rows: vec![DbRow {
         values: vec![
            DbValue::Int(42),
            DbValue::Text("rana".to_string()),
            DbValue::Text("rana@example.com".to_string()),
         ],
      }],
      rows_affected: 1,
   }
}
