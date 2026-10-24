use fyro_db::storage::store::Store;
use fyro_dbcache::protocol::{PROTOCOL_VERSION_3, encode_bind, put_message};
use fyro_dbcache::query::{compute_fingerprint_with_params, parse_sql};
use fyro_dbcache::row::{ColumnInfo, DbResultSet, DbRow, DbValue};
use fyro_dbcache::{DbCacheConfig, DbCacheManager, DbProxyMode};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

fn main() {
   println!("============================================================");
   println!("           FyroDB PostgreSQL Path Benchmark                 ");
   println!("============================================================");

   bench_in_memory_direct();
   bench_encoded_multithread();
   bench_in_memory_complex_joins();
   bench_extended_protocol_tcp();
   bench_tcp_wire_proxy();
}

// Microbenchmarks direct in-memory CustomMap lookup and JSON serialization.
fn bench_in_memory_direct() {
   println!("\n[1] In-Memory Cache Lookup (Single-Table Query)");

   let store = Arc::new(Store::new());
   let manager = Arc::new(DbCacheManager::new(store.clone()));

   let sql = "SELECT id, username, email FROM users WHERE id = 42;";
   let parsed = parse_sql(sql);
   let rs = sample_result_set();

   manager.store_cached_query("db:", &parsed.tables, parsed.fingerprint, &rs, 60_000);

   let threads = 4;
   let ops_per_thread = 500_000;
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

   let elapsed = start.elapsed();
   let qps = (total_ops as f64) / elapsed.as_secs_f64();
   let avg_ns = elapsed.as_nanos() as f64 / (total_ops as f64);

   println!("  Total Queries: {total_ops}");
   println!("  Threads:       {threads}");
   println!("  Duration:      {elapsed:.2?}");
   println!(
      "  Throughput:    {qps:.2} QPS ({:.2}M ops/sec)",
      qps / 1_000_000.0
   );
   println!("  Avg Latency:   {avg_ns:.1} ns / op");
}

// Microbenchmarks the proxy hot path: parse-once analysis plus a pre-encoded
// wire response fetched from the sharded cache, on every core.
fn bench_encoded_multithread() {
   println!("\n[2] Parse-Once + Wire-Encoded Lookup (Multithreaded)");

   let store = Arc::new(Store::new());
   let manager = Arc::new(DbCacheManager::new(store.clone()));

   let sql = "SELECT id, username, email FROM users WHERE id = 42;";
   let parsed = parse_sql(sql);
   let rs = sample_result_set();
   manager.store_cached_query("db:", &parsed.tables, parsed.fingerprint, &rs, 60_000);

   let threads = 4;
   let ops_per_thread = 2_000_000;
   let total_ops = threads * ops_per_thread;

   let start = Instant::now();
   let mut handles = Vec::new();

   for _ in 0..threads {
      let mgr = Arc::clone(&manager);
      let query = sql.to_string();
      handles.push(std::thread::spawn(move || {
         for _ in 0..ops_per_thread {
            let parsed = mgr.analyze(&query);
            let res = mgr.get_encoded("db:", "users", parsed.fingerprint);
            std::hint::black_box(res);
         }
      }));
   }

   for h in handles {
      h.join().unwrap();
   }

   let elapsed = start.elapsed();
   let qps = (total_ops as f64) / elapsed.as_secs_f64();
   let avg_ns = elapsed.as_nanos() as f64 / (total_ops as f64);

   println!("  Total Queries: {total_ops}");
   println!("  Threads:       {threads}");
   println!("  Duration:      {elapsed:.2?}");
   println!(
      "  Throughput:    {qps:.2} QPS ({:.2}M ops/sec)",
      qps / 1_000_000.0
   );
   println!("  Avg Latency:   {avg_ns:.1} ns / op");
   println!("  Parsed SQL cache entries: {}", manager.parsed_cache_len());
}

// Microbenchmarks complex 3-table JOIN queries and AST parsing.
fn bench_in_memory_complex_joins() {
   println!("\n[3] Complex 3-Table JOIN AST Parse + Cache Lookup");

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

   let count = 200_000;
   let start = Instant::now();

   for _ in 0..count {
      let p = parse_sql(join_sql);
      let res = manager.get_cached_query("db:", "users", p.fingerprint);
      std::hint::black_box(res);
   }

   let elapsed = start.elapsed();
   let qps = (count as f64) / elapsed.as_secs_f64();
   let avg_us = elapsed.as_micros() as f64 / (count as f64);

   println!("  Total Queries: {count}");
   println!("  Duration:      {elapsed:.2?}");
   println!("  Throughput:    {qps:.2} QPS (AST parse + memory lookup)");
   println!("  Avg Latency:   {avg_us:.2} µs / op");
}

// Benchmarks the extended query protocol (Parse once, then Bind/Execute/Sync)
// served entirely from the wire cache, with four concurrent clients.
fn bench_extended_protocol_tcp() {
   println!("\n[4] Extended Protocol Cached Round-Trip (Port 15434)");

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
   };

   let sql = "SELECT id, username, email FROM users WHERE id = $1;";
   let parsed = parse_sql(sql);
   let param = Some(b"42".to_vec());
   let fingerprint = compute_fingerprint_with_params(&parsed.normalized, &[param.clone()], &[]);
   let rs = sample_result_set();
   manager.store_cached_query("db:", &parsed.tables, fingerprint, &rs, 60_000);

   let _handle = fyro_dbcache::start_proxy_listener(config, manager.clone()).expect("start proxy");
   std::thread::sleep(Duration::from_millis(50));

   let num_clients = 4;
   let queries_per_client = 25_000;

   let start = Instant::now();
   let completed = Arc::new(AtomicU64::new(0));
   let mut client_handles = Vec::new();

   for _ in 0..num_clients {
      let done = Arc::clone(&completed);
      let query_str = sql.to_string();
      let bind_param = param.clone();
      client_handles.push(std::thread::spawn(move || {
         let mut stream = connect_startup(port);

         // Parse once; the proxy answers locally for cacheable shapes.
         let mut body = Vec::new();
         body.extend_from_slice(b"s1\0");
         body.extend_from_slice(query_str.as_bytes());
         body.push(0);
         body.extend_from_slice(&0i16.to_be_bytes());
         let mut parse_frame = Vec::new();
         put_message(&mut parse_frame, b'P', &body);
         stream.write_all(&parse_frame).unwrap();

         let bind_frame = encode_bind("p1", "s1", &[], &[bind_param], &[]);
         let mut execute_body = b"p1\0".to_vec();
         execute_body.extend_from_slice(&0i32.to_be_bytes());
         let mut execute_frame = Vec::new();
         put_message(&mut execute_frame, b'E', &execute_body);

         for _ in 0..queries_per_client {
            stream.write_all(&bind_frame).unwrap();
            stream.write_all(&execute_frame).unwrap();
            stream.write_all(&[b'S', 0, 0, 0, 4]).unwrap();
            read_until_ready(&mut stream);
            done.fetch_add(1, Ordering::Relaxed);
         }
      }));
   }

   for h in client_handles {
      h.join().unwrap();
   }

   let elapsed = start.elapsed();
   let total = completed.load(Ordering::Relaxed);
   let qps = (total as f64) / elapsed.as_secs_f64();
   let avg_us = elapsed.as_micros() as f64 / (total as f64);

   println!("  Total Extended Queries: {total}");
   println!("  Concurrent Clients:     {num_clients}");
   println!("  Duration:               {elapsed:.2?}");
   println!("  Network Throughput:     {qps:.2} QPS");
   println!("  Avg TCP Latency:        {avg_us:.2} µs / query");
   println!("  Upstream connections:   0 (cache-only mode)");
}

// Benchmarks real PostgreSQL TCP wire protocol round-trip through proxy port.
fn bench_tcp_wire_proxy() {
   println!("\n[5] Simple Query TCP Wire Round-Trip (Port 15433)");

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
   };

   let sql = "SELECT id, username, email FROM users WHERE id = 42;";
   let parsed = parse_sql(sql);
   let rs = sample_result_set();
   manager.store_cached_query("db:", &parsed.tables, parsed.fingerprint, &rs, 60_000);

   let _handle = fyro_dbcache::start_proxy_listener(config, manager.clone()).expect("start proxy");
   std::thread::sleep(Duration::from_millis(50));

   let num_clients = 4;
   let queries_per_client = 25_000;
   let _total_queries = num_clients * queries_per_client;

   let start = Instant::now();
   let completed = Arc::new(AtomicU64::new(0));
   let mut client_handles = Vec::new();

   for _ in 0..num_clients {
      let done = Arc::clone(&completed);
      let query_str = sql.to_string();
      client_handles.push(std::thread::spawn(move || {
         let mut stream = connect_startup(port);

         // Construct Query packet
         let mut q_pkt = Vec::new();
         let q_bytes = query_str.as_bytes();
         let q_len = (q_bytes.len() + 5) as i32;
         q_pkt.push(b'Q');
         q_pkt.extend_from_slice(&q_len.to_be_bytes());
         q_pkt.extend_from_slice(q_bytes);
         q_pkt.push(0);

         for _ in 0..queries_per_client {
            stream.write_all(&q_pkt).unwrap();
            stream.flush().unwrap();
            read_until_ready(&mut stream);
            done.fetch_add(1, Ordering::Relaxed);
         }
      }));
   }

   for h in client_handles {
      h.join().unwrap();
   }

   let elapsed = start.elapsed();
   let total = completed.load(Ordering::Relaxed);
   let qps = (total as f64) / elapsed.as_secs_f64();
   let avg_us = elapsed.as_micros() as f64 / (total as f64);

   println!("  Total TCP Queries: {total}");
   println!("  Concurrent Clients: {num_clients}");
   println!("  Duration:           {elapsed:.2?}");
   println!("  Network Throughput: {qps:.2} QPS");
   println!("  Avg TCP Latency:    {avg_us:.2} µs / query");
   println!("============================================================");
}

fn connect_startup(port: u16) -> TcpStream {
   let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to proxy");
   stream.set_nodelay(true).unwrap();

   let mut startup_payload = Vec::new();
   startup_payload.extend_from_slice(&PROTOCOL_VERSION_3.to_be_bytes());
   startup_payload.extend_from_slice(b"user\0postgres\0database\0test\0\0");
   let len = (startup_payload.len() + 4) as i32;
   stream.write_all(&len.to_be_bytes()).unwrap();
   stream.write_all(&startup_payload).unwrap();
   stream.flush().unwrap();

   read_until_ready(&mut stream);
   stream
}

fn read_until_ready(stream: &mut TcpStream) {
   let mut head = [0u8; 5];
   loop {
      stream.read_exact(&mut head).unwrap();
      let msg_len = i32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
      let mut body = vec![0u8; msg_len - 4];
      stream.read_exact(&mut body).unwrap();
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
