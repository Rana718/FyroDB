#![cfg(feature = "unix-sockets")]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fyro_db::pubsub::PubSub;
use fyro_db::storage::store::Store;
use fyro_db::worker::run_worker;

fn wait_tcp(port: u16) -> TcpStream {
   for _ in 0..100 {
      if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
         stream.set_nodelay(true).unwrap();
         return stream;
      }
      std::thread::sleep(Duration::from_millis(10));
   }
   panic!("failed to connect to tcp port {port}");
}

fn wait_unix(path: &str) -> UnixStream {
   for _ in 0..100 {
      if let Ok(stream) = UnixStream::connect(path) {
         return stream;
      }
      std::thread::sleep(Duration::from_millis(10));
   }
   panic!("failed to connect to unix socket {path}");
}

trait SocketIO: Read + Write {
   fn set_timeout(&self, dur: Option<Duration>);
}

impl SocketIO for TcpStream {
   fn set_timeout(&self, dur: Option<Duration>) {
      let _ = self.set_read_timeout(dur);
      let _ = self.set_write_timeout(dur);
   }
}

impl SocketIO for UnixStream {
   fn set_timeout(&self, dur: Option<Duration>) {
      let _ = self.set_read_timeout(dur);
      let _ = self.set_write_timeout(dur);
   }
}

struct LatencyStats {
   avg_us: f64,
   p50_us: f64,
   p95_us: f64,
   p99_us: f64,
   ops_sec: f64,
}

fn bench_ping_pong<S: SocketIO>(mut stream: S, iterations: usize) -> LatencyStats {
   stream.set_timeout(Some(Duration::from_secs(5)));
   let ping_cmd = b"*1\r\n$4\r\nPING\r\n";
   let mut reply_buf = [0u8; 7]; // +PONG\r\n
   let mut latencies: Vec<f64> = Vec::with_capacity(iterations);

   // Warmup
   for _ in 0..500 {
      stream.write_all(ping_cmd).unwrap();
      stream.read_exact(&mut reply_buf).unwrap();
   }

   let start_total = Instant::now();
   for _ in 0..iterations {
      let t0 = Instant::now();
      stream.write_all(ping_cmd).unwrap();
      stream.read_exact(&mut reply_buf).unwrap();
      let elapsed_us = t0.elapsed().as_nanos() as f64 / 1000.0;
      latencies.push(elapsed_us);
   }
   let total_duration = start_total.elapsed();

   latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
   let sum: f64 = latencies.iter().sum();
   let avg_us = sum / (latencies.len() as f64);
   let p50_us = latencies[(latencies.len() as f64 * 0.50) as usize];
   let p95_us = latencies[(latencies.len() as f64 * 0.95) as usize];
   let p99_us = latencies[(latencies.len() as f64 * 0.99) as usize];
   let ops_sec = (iterations as f64) / total_duration.as_secs_f64();

   LatencyStats {
      avg_us,
      p50_us,
      p95_us,
      p99_us,
      ops_sec,
   }
}

fn bench_pipeline<S: SocketIO>(
   mut stream: S,
   total_ops: usize,
   batch_size: usize,
   is_set: bool,
) -> f64 {
   stream.set_timeout(Some(Duration::from_secs(10)));
   let mut req_buf = Vec::with_capacity(batch_size * 64);
   let mut resp_buf = vec![0u8; batch_size * 64];

   let mut sent = 0;
   let start = Instant::now();

   while sent < total_ops {
      let cur_batch = batch_size.min(total_ops - sent);
      req_buf.clear();

      for j in 0..cur_batch {
         let key_idx = (sent + j) % 10_000;
         if is_set {
            let cmd = format!(
               "*3\r\n$3\r\nSET\r\n$8\r\nkey_{:04}\r\n$5\r\nvalue\r\n",
               key_idx
            );
            req_buf.extend_from_slice(cmd.as_bytes());
         } else {
            let cmd = format!("*2\r\n$3\r\nGET\r\n$8\r\nkey_{:04}\r\n", key_idx);
            req_buf.extend_from_slice(cmd.as_bytes());
         }
      }

      stream.write_all(&req_buf).unwrap();

      let expected_bytes = if is_set {
         cur_batch * 5 // +OK\r\n
      } else {
         cur_batch * 11 // $5\r\nvalue\r\n
      };

      let mut recvd = 0;
      while recvd < expected_bytes {
         match stream.read(&mut resp_buf[recvd..expected_bytes]) {
            Ok(0) => break,
            Ok(n) => recvd += n,
            Err(e) => {
               let recvd_str = String::from_utf8_lossy(&resp_buf[..recvd]);
               panic!(
                  "bench_pipeline read error: {:?}, expected {}, received {} bytes: {:?}",
                  e, expected_bytes, recvd, recvd_str
               );
            }
         }
      }
      sent += cur_batch;
   }

   let elapsed = start.elapsed();
   (total_ops as f64) / elapsed.as_secs_f64()
}

#[test]
#[cfg(feature = "unix-sockets")]
fn test_benchmark_tcp_vs_unix() {
   let sock_path = format!("/tmp/fyrodb_perf_{}.sock", std::process::id());
   let _ = std::fs::remove_file(&sock_path);

   // Find ephemeral port for TCP
   let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
   let tcp_port = probe.local_addr().unwrap().port();
   drop(probe);

   let store = Arc::new(Store::with_config(4, usize::MAX));
   let pubsub = Arc::new(PubSub::new());

   // Start TCP worker
   let s_tcp = Arc::clone(&store);
   let p_tcp = Arc::clone(&pubsub);
   std::thread::Builder::new()
      .name("tcp-worker".into())
      .spawn(move || {
         run_worker(&s_tcp, &p_tcp, tcp_port, "127.0.0.1", None, 0);
      })
      .unwrap();

   // Start Unix worker
   let s_unix = Arc::clone(&store);
   let p_unix = Arc::clone(&pubsub);
   let b_unix = format!("unix:{}", sock_path);
   std::thread::Builder::new()
      .name("unix-worker".into())
      .spawn(move || {
         run_worker(&s_unix, &p_unix, 0, &b_unix, None, 1);
      })
      .unwrap();

   // Connect
   let tcp_client = wait_tcp(tcp_port);
   let unix_client = wait_unix(&sock_path);

   println!("\n================================================================================");
   println!("                  FYRODB: TCP vs UNIX DOMAIN SOCKET BENCHMARK                   ");
   println!("================================================================================");

   // 1. Latency (Ping-Pong, batch = 1)
   let ping_iters = 30_000;
   println!("\n[1] PING-PONG LATENCY (Batch = 1, {ping_iters} round-trips)");
   println!("--------------------------------------------------------------------------------");
   let tcp_ping = bench_ping_pong(tcp_client, ping_iters);
   let unix_ping = bench_ping_pong(unix_client, ping_iters);

   println!(
      "Transport    | Throughput     | Avg Latency | p50 Latency | p95 Latency | p99 Latency"
   );
   println!(
      "-------------|----------------|-------------|-------------|-------------|------------"
   );
   println!(
      "TCP (127.0.1)| {:>10.0} op/s | {:>8.2} µs | {:>8.2} µs | {:>8.2} µs | {:>8.2} µs",
      tcp_ping.ops_sec, tcp_ping.avg_us, tcp_ping.p50_us, tcp_ping.p95_us, tcp_ping.p99_us
   );
   println!(
      "UNIX Socket  | {:>10.0} op/s | {:>8.2} µs | {:>8.2} µs | {:>8.2} µs | {:>8.2} µs",
      unix_ping.ops_sec, unix_ping.avg_us, unix_ping.p50_us, unix_ping.p95_us, unix_ping.p99_us
   );
   let lat_reduction = (tcp_ping.avg_us - unix_ping.avg_us) / tcp_ping.avg_us * 100.0;
   let qps_speedup = unix_ping.ops_sec / tcp_ping.ops_sec;
   println!(
      "--> UNIX Socket is {:.2}x faster ({:.1}% lower latency)",
      qps_speedup, lat_reduction
   );

   // 2. Pipelined SET
   let pipeline_ops = 100_000;
   println!("\n[2] PIPELINED SET THROUGHPUT ({pipeline_ops} ops)");
   println!("--------------------------------------------------------------------------------");
   for batch in [8, 16, 64] {
      let tcp_c = wait_tcp(tcp_port);
      let unix_c = wait_unix(&sock_path);
      let tcp_qps = bench_pipeline(tcp_c, pipeline_ops, batch, true);
      let unix_qps = bench_pipeline(unix_c, pipeline_ops, batch, true);
      let speedup = unix_qps / tcp_qps;
      println!(
         "Batch = {:>2}  |  TCP: {:>10.0} ops/sec  |  UNIX: {:>10.0} ops/sec  |  Speedup: {:.2}x",
         batch, tcp_qps, unix_qps, speedup
      );
   }

   // 3. Pipelined GET
   println!("\n[3] PIPELINED GET THROUGHPUT ({pipeline_ops} ops)");
   println!("--------------------------------------------------------------------------------");
   for batch in [8, 16, 64] {
      let tcp_c = wait_tcp(tcp_port);
      let unix_c = wait_unix(&sock_path);
      let tcp_qps = bench_pipeline(tcp_c, pipeline_ops, batch, false);
      let unix_qps = bench_pipeline(unix_c, pipeline_ops, batch, false);
      let speedup = unix_qps / tcp_qps;
      println!(
         "Batch = {:>2}  |  TCP: {:>10.0} ops/sec  |  UNIX: {:>10.0} ops/sec  |  Speedup: {:.2}x",
         batch, tcp_qps, unix_qps, speedup
      );
   }
   println!("================================================================================\n");

   let _ = std::fs::remove_file(&sock_path);
}
