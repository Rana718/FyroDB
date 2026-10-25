//! bench_all_v — FyroDB RESP + dbcache PostgreSQL proxy combined benchmark.
//!
//! Benchmarks both subsystems in a single run and prints a unified summary
//! table. Vector benchmarks are intentionally excluded.
//!
//! Usage:
//!   cargo run --release --features bench-tools --bin bench_all_v [-- --smoke]
//!
//! Environment:
//!   FYRODB_WORKERS=N   override RESP worker count (default: num_cpus)

use fyro_db::storage::store::Store;
use fyro_dbcache::protocol::{encode_bind, put_message, PROTOCOL_VERSION_3};
use fyro_dbcache::query::{compute_fingerprint_with_params, parse_sql};
use fyro_dbcache::row::{ColumnInfo, DbResultSet, DbRow, DbValue};
use fyro_dbcache::{DbCacheConfig, DbCacheManager, DbProxyMode};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

// ── Result collection ─────────────────────────────────────────────────────────

struct Row {
   section: &'static str,
   label: &'static str,
   ops: u64,
   elapsed: Duration,
}

impl Row {
   fn rate(&self) -> f64 {
      self.ops as f64 / self.elapsed.as_secs_f64()
   }
}

fn fmt_rate(r: f64) -> String {
   if r >= 1e9 { format!("{:.2}B ops/s", r / 1e9) }
   else if r >= 1e6 { format!("{:.2}M ops/s", r / 1e6) }
   else if r >= 1e3 { format!("{:.1}k ops/s", r / 1e3) }
   else { format!("{:.0} ops/s", r) }
}

fn print_summary(rows: &[Row]) {
   const W_SEC: usize = 10;
   const W_LBL: usize = 38;
   const W_EL: usize = 9;
   const W_TP: usize = 16;
   println!();
   println!("══ Summary ═══════════════════════════════════════════════════════════════");
   println!("  {:<W_SEC$}  {:<W_LBL$}  {:>W_EL$}  {:>W_TP$}", "Section", "Benchmark", "Elapsed", "Throughput");
   println!("  {:<W_SEC$}  {:<W_LBL$}  {:>W_EL$}  {:>W_TP$}",
      "─".repeat(W_SEC), "─".repeat(W_LBL), "─".repeat(W_EL), "─".repeat(W_TP));
   let mut last = "";
   for r in rows {
      let sec = if r.section != last { last = r.section; r.section } else { "" };
      println!(
         "  {:<W_SEC$}  {:<W_LBL$}  {:>W_EL$}  {:>W_TP$}",
         sec, r.label,
         format!("{:.0}ms", r.elapsed.as_millis()),
         fmt_rate(r.rate()),
      );
   }
   println!("══════════════════════════════════════════════════════════════════════════");
}

// ── RESP (FyroDB) TCP helpers ─────────────────────────────────────────────────

fn resp_connect(port: u16) -> TcpStream {
   let s = TcpStream::connect(("127.0.0.1", port)).expect("resp connect");
   s.set_nodelay(true).unwrap();
   s
}

fn build_set_batch(base: usize, n: usize, out: &mut Vec<u8>) {
   out.clear();
   for i in 0..n {
      let k = format!("bk{}", base + i);
      write!(out, "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$5\r\nvalue\r\n", k.len(), k).unwrap();
   }
}

fn build_get_batch(base: usize, n: usize, out: &mut Vec<u8>) {
   out.clear();
   for i in 0..n {
      let k = format!("bk{}", base + i);
      write!(out, "*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", k.len(), k).unwrap();
   }
}

/// Pipelined RESP benchmark. reply_bytes: 5 for SET (+OK\r\n), 11 for GET ($5\r\nvalue\r\n).
fn bench_resp(
   port: u16,
   clients: usize,
   ops: usize,
   batch: usize,
   is_set: bool,
   reply_bytes: usize,
) -> Duration {
   let start = Instant::now();
   std::thread::scope(|scope| {
      for id in 0..clients {
         scope.spawn(move || {
            let mut w = resp_connect(port);
            let mut r = w.try_clone().unwrap();
            let expected = ops * reply_bytes;
            std::thread::scope(|inner| {
               inner.spawn(move || {
                  let mut buf = [0u8; 256 * 1024];
                  let mut got = 0usize;
                  while got < expected {
                     match r.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => got += n,
                     }
                  }
               });
               let mut send = Vec::with_capacity(batch * 40);
               let mut sent = 0usize;
               while sent < ops {
                  let n = batch.min(ops - sent);
                  if is_set { build_set_batch(id * ops + sent, n, &mut send); }
                  else { build_get_batch(id * ops + sent, n, &mut send); }
                  if w.write_all(&send).is_err() { break; }
                  sent += n;
               }
            });
         });
      }
   });
   start.elapsed()
}

// ── PostgreSQL wire (dbcache) TCP helpers ─────────────────────────────────────

fn pg_connect(port: u16) -> TcpStream {
   let mut s = TcpStream::connect(("127.0.0.1", port)).expect("pg connect");
   s.set_nodelay(true).unwrap();
   let mut payload = PROTOCOL_VERSION_3.to_be_bytes().to_vec();
   payload.extend_from_slice(b"user\0postgres\0database\0test\0\0");
   let len = (payload.len() + 4) as i32;
   s.write_all(&len.to_be_bytes()).unwrap();
   s.write_all(&payload).unwrap();
   s.flush().unwrap();
   pg_drain_ready(&mut s);
   s
}

fn pg_drain_ready(s: &mut TcpStream) {
   let mut head = [0u8; 5];
   loop {
      s.read_exact(&mut head).unwrap();
      let body_len = i32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize - 4;
      let mut body = vec![0u8; body_len];
      s.read_exact(&mut body).unwrap();
      if head[0] == b'Z' { break; }
   }
}

fn bench_pg_simple(port: u16, clients: usize, ops: usize, sql: &str) -> Duration {
   let q = sql.as_bytes();
   let q_len = (q.len() + 5) as i32;
   let mut frame = Vec::with_capacity(q.len() + 5);
   frame.push(b'Q');
   frame.extend_from_slice(&q_len.to_be_bytes());
   frame.extend_from_slice(q);
   frame.push(0u8);
   let frame = Arc::new(frame);

   let done = Arc::new(AtomicU64::new(0));
   let start = Instant::now();
   let mut handles = Vec::with_capacity(clients);
   for _ in 0..clients {
      let f = Arc::clone(&frame);
      let d = Arc::clone(&done);
      handles.push(std::thread::spawn(move || {
         let mut s = pg_connect(port);
         for _ in 0..ops {
            s.write_all(&f).unwrap();
            s.flush().unwrap();
            pg_drain_ready(&mut s);
            d.fetch_add(1, Ordering::Relaxed);
         }
      }));
   }
   for h in handles { h.join().unwrap(); }
   start.elapsed()
}

fn bench_pg_extended(port: u16, clients: usize, ops: usize, sql: &str) -> Duration {
   let param: Option<Vec<u8>> = Some(b"42".to_vec());

   // Parse frame: named statement "s1"
   let mut parse_body = Vec::new();
   parse_body.extend_from_slice(b"s1\0");
   parse_body.extend_from_slice(sql.as_bytes());
   parse_body.push(0u8);
   parse_body.extend_from_slice(&0i16.to_be_bytes());
   let mut parse_frame = Vec::new();
   put_message(&mut parse_frame, b'P', &parse_body);

   // Bind frame: portal "p1", statement "s1", param = "42"
   let bind_frame = encode_bind("p1", "s1", &[], &[param], &[]);

   // Execute frame: portal "p1", max_rows = 0
   let mut exec_body = b"p1\0".to_vec();
   exec_body.extend_from_slice(&0i32.to_be_bytes());
   let mut exec_frame = Vec::new();
   put_message(&mut exec_frame, b'E', &exec_body);

   let sync_frame: Vec<u8> = vec![b'S', 0, 0, 0, 4];

   let parse_frame = Arc::new(parse_frame);
   let bind_frame = Arc::new(bind_frame);
   let exec_frame = Arc::new(exec_frame);
   let sync_frame = Arc::new(sync_frame);
   let done = Arc::new(AtomicU64::new(0));

   let start = Instant::now();
   let mut handles = Vec::with_capacity(clients);
   for _ in 0..clients {
      let pf = Arc::clone(&parse_frame);
      let bf = Arc::clone(&bind_frame);
      let ef = Arc::clone(&exec_frame);
      let sf = Arc::clone(&sync_frame);
      let d = Arc::clone(&done);
      handles.push(std::thread::spawn(move || {
         let mut s = pg_connect(port);
         // Send Parse once — no Sync yet; proxy buffers it.
         s.write_all(&pf).unwrap();
         // Loop: Bind + Execute + Sync; first Sync also flushes the buffered Parse.
         for _ in 0..ops {
            s.write_all(&bf).unwrap();
            s.write_all(&ef).unwrap();
            s.write_all(&sf).unwrap();
            s.flush().unwrap();
            pg_drain_ready(&mut s);
            d.fetch_add(1, Ordering::Relaxed);
         }
      }));
   }
   for h in handles { h.join().unwrap(); }
   start.elapsed()
}

// ── Shared result set ─────────────────────────────────────────────────────────

fn sample_rs() -> DbResultSet {
   DbResultSet {
      columns: vec![
         ColumnInfo { name: "id".into(), type_oid: 23 },
         ColumnInfo { name: "username".into(), type_oid: 25 },
         ColumnInfo { name: "email".into(), type_oid: 25 },
      ],
      rows: vec![DbRow {
         values: vec![
            DbValue::Int(42),
            DbValue::Text("rana".into()),
            DbValue::Text("rana@example.com".into()),
         ],
      }],
      rows_affected: 1,
   }
}

// ── Main ──────────────────────────────────────────────────────────────────────

fn main() {
   let smoke = std::env::args().any(|a| a == "--smoke" || a == "-s");

   // Scale: (pg_clients, pg_ops, resp_clients, resp_ops, batch)
   let (pg_c, pg_ops, resp_c, resp_ops, batch) = if smoke {
      (2usize, 500usize, 4usize, 20_000usize, 500usize)
   } else {
      (4usize, 25_000usize, 8usize, 500_000usize, 2_500usize)
   };

   let workers: usize = std::env::var("FYRODB_WORKERS")
      .ok()
      .and_then(|v| v.parse().ok())
      .filter(|&w: &usize| w > 0)
      .unwrap_or_else(num_cpus::get);

   println!("══════════════════════════════════════════════════════════════════════════");
   println!("          FyroDB + dbcache  Combined  Benchmark   (bench-all-v)          ");
   println!("══════════════════════════════════════════════════════════════════════════");
   println!("  RESP workers: {workers} | RESP clients: {resp_c}×{resp_ops} ops");
   println!("  PG proxy clients: {pg_c}×{pg_ops} queries (cache-hit only)");
   if smoke { println!("  (smoke mode)"); }
   println!("══════════════════════════════════════════════════════════════════════════\n");

   let mut rows: Vec<Row> = Vec::new();

   // ── FyroDB RESP server ────────────────────────────────────────────────────
   let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
   let resp_port = probe.local_addr().unwrap().port();
   drop(probe);

   let resp_store = Arc::new(Store::with_config(32, 2_000_000));
   let pubsub = Arc::new(fyro_db::pubsub::PubSub::new());
   for idx in 0..workers {
      let s = Arc::clone(&resp_store);
      let p = Arc::clone(&pubsub);
      std::thread::Builder::new()
         .name(format!("bench-resp-{idx}"))
         .spawn(move || fyro_db::worker::run_worker(&s, &p, resp_port, "127.0.0.1", None, idx))
         .unwrap();
   }
   std::thread::sleep(Duration::from_millis(100));

   // RESP warmup ping
   {
      let mut w = resp_connect(resp_port);
      w.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
      let mut pong = [0u8; 7];
      w.read_exact(&mut pong).unwrap();
   }

   // ── Section 1: FyroDB RESP ────────────────────────────────────────────────
   let total_resp = (resp_c * resp_ops) as u64;
   println!("── FyroDB RESP  ({resp_c} clients × {resp_ops} ops, batch {batch}) ─────────────────────────────");

   let el = bench_resp(resp_port, resp_c, resp_ops, batch, true, 5);
   println!("  SET pipelined    {:>7.0}ms  {}", el.as_millis(), fmt_rate(total_resp as f64 / el.as_secs_f64()));
   rows.push(Row { section: "FyroDB", label: "SET pipelined", ops: total_resp, elapsed: el });

   // GET: keys were populated by the SET run above (same key range)
   let el = bench_resp(resp_port, resp_c, resp_ops, batch, false, 11);
   println!("  GET pipelined    {:>7.0}ms  {}\n", el.as_millis(), fmt_rate(total_resp as f64 / el.as_secs_f64()));
   rows.push(Row { section: "FyroDB", label: "GET pipelined", ops: total_resp, elapsed: el });

   // ── dbcache proxy servers ─────────────────────────────────────────────────
   let pg_store = Arc::new(Store::new());
   let pg_mgr = Arc::new(DbCacheManager::new(pg_store.clone()));

   let sql_simple = "SELECT id, username, email FROM users WHERE id = 42;";
   let ps = parse_sql(sql_simple);
   pg_mgr.store_cached_query("db:", &ps.tables, ps.fingerprint, &sample_rs(), 60_000);

   let sql_ext = "SELECT id, username, email FROM users WHERE id = $1;";
   let pe = parse_sql(sql_ext);
   let fp_ext = compute_fingerprint_with_params(
      &pe.normalized,
      std::slice::from_ref(&Some(b"42".to_vec())),
      &[],
   );
   pg_mgr.store_cached_query("db:", &pe.tables, fp_ext, &sample_rs(), 60_000);

   // Allocate two ports: one per proxy instance (simple vs extended)
   let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
   let port_simple = probe.local_addr().unwrap().port();
   drop(probe);
   let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
   let port_ext = probe.local_addr().unwrap().port();
   drop(probe);

   let base_cfg = DbCacheConfig {
      enabled: true,
      mode: DbProxyMode::Dual,
      upstream_addr: None,
      listen_addr: String::new(), // overridden below
      default_ttl_ms: 60_000,
      key_prefix: "db:".into(),
      flush_on_start: true,
      sync_interval_ms: 0,
      cache_max_keys: 100_000,
      ttl_jitter_pct: 0,
      ..DbCacheConfig::default()
   };

   let _ph1 = fyro_dbcache::start_proxy_listener(
      DbCacheConfig { listen_addr: format!("127.0.0.1:{port_simple}"), ..base_cfg.clone() },
      Arc::clone(&pg_mgr),
   )
   .expect("start simple proxy");

   let _ph2 = fyro_dbcache::start_proxy_listener(
      DbCacheConfig { listen_addr: format!("127.0.0.1:{port_ext}"), ..base_cfg },
      Arc::clone(&pg_mgr),
   )
   .expect("start ext proxy");

   std::thread::sleep(Duration::from_millis(50));

   // ── Section 2: dbcache ────────────────────────────────────────────────────
   let total_pg = (pg_c * pg_ops) as u64;
   println!("── dbcache proxy  ({pg_c} clients × {pg_ops} queries, cache-hit only) ─────────────────────");

   let el = bench_pg_simple(port_simple, pg_c, pg_ops, sql_simple);
   println!("  Simple query     {:>7.0}ms  {}", el.as_millis(), fmt_rate(total_pg as f64 / el.as_secs_f64()));
   rows.push(Row { section: "dbcache", label: "simple query (cache hit)", ops: total_pg, elapsed: el });

   let el = bench_pg_extended(port_ext, pg_c, pg_ops, sql_ext);
   println!("  Extended proto   {:>7.0}ms  {}\n", el.as_millis(), fmt_rate(total_pg as f64 / el.as_secs_f64()));
   rows.push(Row { section: "dbcache", label: "extended protocol (cache hit)", ops: total_pg, elapsed: el });

   print_summary(&rows);
}
