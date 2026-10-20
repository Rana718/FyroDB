use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

fn set_quickack(stream: &TcpStream) {
   #[cfg(target_os = "linux")]
   {
      use std::os::unix::io::AsRawFd;
      let fd = stream.as_raw_fd();
      unsafe {
         let opt: libc::c_int = 1;
         libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_QUICKACK,
            &opt as *const _ as *const libc::c_void,
            std::mem::size_of_val(&opt) as libc::socklen_t,
         );
      }
   }
}

fn dial(port: u16) -> TcpStream {
   let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
   stream.set_nodelay(true).unwrap();
   set_quickack(&stream);
   let socket = socket2::SockRef::from(&stream);
   let _ = socket.set_recv_buffer_size(2 * 1024 * 1024);
   let _ = socket.set_send_buffer_size(2 * 1024 * 1024);
   stream
}

fn build_batch_set(base: usize, count: usize, buf: &mut Vec<u8>) {
   buf.clear();
   for j in 0..count {
      let n = base + j;
      let key = format!("key_{}", n);
      let cmd = format!("*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$5\r\nvalue\r\n", key.len(), key);
      buf.extend_from_slice(cmd.as_bytes());
   }
}

fn build_batch_get(base: usize, count: usize, buf: &mut Vec<u8>) {
   buf.clear();
   for j in 0..count {
      let n = base + j;
      let key = format!("key_{}", n);
      let cmd = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
      buf.extend_from_slice(cmd.as_bytes());
   }
}

fn run_pipeline(
   mut stream: TcpStream,
   total_ops: usize,
   batch_size: usize,
   reply_bytes: usize,
   is_set: bool,
   base_offset: usize,
) {
   let mut read_stream = stream.try_clone().unwrap();
   let total_expected_bytes = total_ops * reply_bytes;
   let done = Arc::new(AtomicBool::new(false));
   let done_reader = Arc::clone(&done);

   std::thread::scope(|s| {
      // Reader thread
      s.spawn(move || {
         let mut buf = vec![0u8; 256 * 1024];
         let mut received = 0usize;
         while received < total_expected_bytes {
            match read_stream.read(&mut buf) {
               Ok(0) => break,
               Ok(n) => {
                  received += n;
                  set_quickack(&read_stream);
               }
               Err(_) => break,
            }
         }
         done_reader.store(true, Ordering::Release);
      });

      // Writer thread
      let mut send_buf = Vec::with_capacity(batch_size * 64);
      let mut sent = 0usize;
      while sent < total_ops {
         let n = batch_size.min(total_ops - sent);
         if is_set {
            build_batch_set(base_offset + sent, n, &mut send_buf);
         } else {
            build_batch_get(base_offset + sent, n, &mut send_buf);
         }
         if stream.write_all(&send_buf).is_err() {
            break;
         }
         sent += n;
      }
   });
}

fn main() {
   let args: Vec<String> = std::env::args().collect();
   let mut port = 0u16;
   let mut clients = 100usize;
   let mut ops_per_client = 100_000usize;
   let mut pipe_size = 2500usize;

   let mut i = 1;
   while i < args.len() {
      match args[i].as_str() {
         "-p" | "--port" => {
            port = args[i + 1].parse().unwrap();
            i += 2;
         }
         "-c" | "--clients" => {
            clients = args[i + 1].parse().unwrap();
            i += 2;
         }
         "-n" | "--ops" => {
            ops_per_client = args[i + 1].parse().unwrap();
            i += 2;
         }
         "--pipe" => {
            pipe_size = args[i + 1].parse().unwrap();
            i += 2;
         }
         _ => {
            i += 1;
         }
      }
   }

   let _server_guard;
   if port == 0 {
      // Start internal Null-Store server
      fyro_db::handler::conn::set_null_store(true);
      let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
      port = probe.local_addr().unwrap().port();
      drop(probe);

      let workers = num_cpus::get();
      let store = Arc::new(fyro_db::storage::store::Store::with_config(32, 2_000_000));
      let pubsub = Arc::new(fyro_db::pubsub::PubSub::new());

      for idx in 0..workers {
         let s = Arc::clone(&store);
         let p = Arc::clone(&pubsub);
         std::thread::Builder::new()
            .name(format!("worker-{}", idx))
            .spawn(move || {
               fyro_db::worker::run_worker(&s, &p, port, "127.0.0.1", None, idx);
            })
            .unwrap();
      }
      std::thread::sleep(std::time::Duration::from_millis(150));
      _server_guard = Some(store);
   }

   let total_ops = clients * ops_per_client;

   println!("==========================================================================");
   println!("           FYRODB PURE TCP PROTOCOL LAYER BENCHMARK");
   println!("   Server: 127.0.0.1:{} | Workers: {} | Clients: {}", port, num_cpus::get(), clients);
   println!("   Total Ops: {} | Ops/Client: {} | Pipeline Batch: {}", total_ops, ops_per_client, pipe_size);
   println!("==========================================================================\n");

   // Warmup
   let mut warmup = dial(port);
   warmup.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
   let mut ping_resp = [0u8; 7];
   warmup.read_exact(&mut ping_resp).unwrap();
   drop(warmup);

   // 1. Pipeline-64 SET
   let mut conns = Vec::with_capacity(clients);
   for _ in 0..clients {
      conns.push(dial(port));
   }
   println!("Running Pipeline-64 SET...");
   let start_seq = Instant::now();
   std::thread::scope(|s| {
      for (id, stream) in conns.into_iter().enumerate() {
         s.spawn(move || {
            run_pipeline(stream, ops_per_client, 64, 5, true, id * ops_per_client);
         });
      }
   });
   let seq_el = start_seq.elapsed();
   let seq_rate = total_ops as f64 / seq_el.as_secs_f64() / 1e6;
   println!("  Pipeline-64 SET: {:.2?} ({:.2}M ops/sec)", seq_el, seq_rate);

   // 2. Pipelined SET (batch_size = pipe_size)
   let mut conns = Vec::with_capacity(clients);
   for _ in 0..clients {
      conns.push(dial(port));
   }
   println!("Running Pipelined SET (batch={})...", pipe_size);
   let start_set = Instant::now();
   std::thread::scope(|s| {
      for (id, stream) in conns.into_iter().enumerate() {
         s.spawn(move || {
            run_pipeline(stream, ops_per_client, pipe_size, 5, true, id * ops_per_client);
         });
      }
   });
   let set_el = start_set.elapsed();
   let set_rate = total_ops as f64 / set_el.as_secs_f64() / 1e6;
   println!("  Pipelined SET:   {:.2?} ({:.2}M ops/sec)", set_el, set_rate);

   // 3. Pipelined GET (batch_size = pipe_size)
   let mut conns = Vec::with_capacity(clients);
   for _ in 0..clients {
      conns.push(dial(port));
   }
   println!("Running Pipelined GET (batch={})...", pipe_size);
   let start_get = Instant::now();
   std::thread::scope(|s| {
      for (id, stream) in conns.into_iter().enumerate() {
         s.spawn(move || {
            run_pipeline(stream, ops_per_client, pipe_size, 11, false, id * ops_per_client);
         });
      }
   });
   let get_el = start_get.elapsed();
   let get_rate = total_ops as f64 / get_el.as_secs_f64() / 1e6;
   println!("  Pipelined GET:   {:.2?} ({:.2}M ops/sec)", get_el, get_rate);

   println!("\n── Protocol Summary ──────────────────────────────────────────────");
   println!("  Pipeline-64 SET: {:>8.2?}   {:>8.2}M ops/sec", seq_el, seq_rate);
   println!("  Pipelined SET:   {:>8.2?}   {:>8.2}M ops/sec", set_el, set_rate);
   println!("  Pipelined GET:   {:>8.2?}   {:>8.2}M ops/sec", get_el, get_rate);
   println!("==================================================================\n");
}
