use socket2::{Domain, Protocol, Socket, Type};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
      let cmd = format!(
         "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$5\r\nvalue\r\n",
         key.len(),
         key
      );
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

// ── LAYER 1: Raw Linux Kernel TCP Benchmark ───────────────────────────
fn run_raw_kernel_tcp(workers: usize, clients: usize, ops_per_client: usize, batch_size: usize) {
   let probe = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).unwrap();
   probe.set_reuse_address(true).unwrap();
   probe.set_reuse_port(true).unwrap();
   let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
   probe.bind(&addr.into()).unwrap();
   let port = probe.local_addr().unwrap().as_socket_ipv4().unwrap().port();
   drop(probe);

   let server_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
   let total_ops = clients * ops_per_client;
   let msg_size = 40usize; // ~40 bytes per command
   let total_bytes = (total_ops * msg_size) as u64;

   let stop = Arc::new(AtomicBool::new(false));
   let received_bytes = Arc::new(AtomicU64::new(0));

   let mut server_handles = Vec::new();
   for _ in 0..workers {
      let stop_w = Arc::clone(&stop);
      let rx_w = Arc::clone(&received_bytes);
      let s_addr = server_addr;
      let handle = std::thread::spawn(move || {
         let sock = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).unwrap();
         sock.set_reuse_address(true).unwrap();
         sock.set_reuse_port(true).unwrap();
         let _ = sock.set_recv_buffer_size(2 * 1024 * 1024);
         sock.bind(&s_addr.into()).unwrap();
         sock.listen(8192).unwrap();
         let listener: TcpListener = sock.into();
         listener.set_nonblocking(true).unwrap();

         let mut conns = Vec::new();
         let mut buf = vec![0u8; 256 * 1024];

         while !stop_w.load(Ordering::Relaxed) {
            while let Ok((stream, _)) = listener.accept() {
               let _ = stream.set_nonblocking(true);
               conns.push(stream);
            }
            let mut i = 0;
            while i < conns.len() {
               match conns[i].read(&mut buf) {
                  Ok(0) => {
                     conns.swap_remove(i);
                  }
                  Ok(n) => {
                     rx_w.fetch_add(n as u64, Ordering::Relaxed);
                     i += 1;
                  }
                  Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                     i += 1;
                  }
                  Err(_) => {
                     conns.swap_remove(i);
                  }
               }
            }
            if conns.is_empty() {
               std::thread::yield_now();
            }
         }
      });
      server_handles.push(handle);
   }

   std::thread::sleep(std::time::Duration::from_millis(50));

   // Connect clients
   let mut client_streams = Vec::with_capacity(clients);
   for _ in 0..clients {
      client_streams.push(dial(port));
   }

   let start = Instant::now();
   std::thread::scope(|s| {
      for mut stream in client_streams {
         s.spawn(move || {
            let send_chunk = vec![b'x'; batch_size * msg_size];
            let mut sent_ops = 0usize;
            while sent_ops < ops_per_client {
               let n = batch_size.min(ops_per_client - sent_ops);
               let slice = &send_chunk[..n * msg_size];
               if stream.write_all(slice).is_err() {
                  break;
               }
               sent_ops += n;
            }
         });
      }
   });

   while received_bytes.load(Ordering::Relaxed) < total_bytes && start.elapsed().as_secs() < 10 {
      std::thread::yield_now();
   }
   let el = start.elapsed();
   stop.store(true, Ordering::Relaxed);

   let rx = received_bytes.load(Ordering::Relaxed);
   let gb = rx as f64 / (1024.0 * 1024.0 * 1024.0);
   let gbs = gb / el.as_secs_f64();
   let gbps = (rx as f64 * 8.0) / el.as_secs_f64() / 1e9;
   let ops_rate = (rx / msg_size as u64) as f64 / el.as_secs_f64() / 1e6;

   println!(
      "  Raw Kernel TCP Pipeline ({} clients, {} workers):",
      clients, workers
   );
   println!(
      "    Elapsed: {:?} | Rate: {:.2}M msgs/sec | Bandwidth: {:.2} GB/s ({:.2} Gbps)\n",
      el, ops_rate, gbs, gbps
   );
}

// ── LAYER 2: FyroDB TCP Ingestion (Arrival at Parser, Zero Parsing) ────
fn run_fyrodb_arrival(port: u16, clients: usize, ops_per_client: usize, batch_size: usize) {
   fyro_db::handler::conn::set_raw_arrival_only(true);
   fyro_db::handler::conn::set_null_store(false);
   fyro_db::handler::conn::reset_raw_arrival_bytes();

   let total_ops = clients * ops_per_client;
   let mut conns = Vec::with_capacity(clients);
   for _ in 0..clients {
      conns.push(dial(port));
   }

   let total_sent_bytes = Arc::new(AtomicU64::new(0));

   let start = Instant::now();
   std::thread::scope(|s| {
      for (id, mut stream) in conns.into_iter().enumerate() {
         let sent_bytes = Arc::clone(&total_sent_bytes);
         s.spawn(move || {
            let mut send_buf = Vec::with_capacity(batch_size * 64);
            let mut sent = 0usize;
            let base = id * ops_per_client;
            while sent < ops_per_client {
               let n = batch_size.min(ops_per_client - sent);
               build_batch_set(base + sent, n, &mut send_buf);
               if stream.write_all(&send_buf).is_err() {
                  break;
               }
               sent_bytes.fetch_add(send_buf.len() as u64, Ordering::Relaxed);
               sent += n;
            }
         });
      }
   });

   // Wait until all bytes have arrived into FyroDB worker read buffers
   let expected = total_sent_bytes.load(Ordering::Relaxed);
   while fyro_db::handler::conn::get_raw_arrival_bytes() < expected
      && start.elapsed().as_secs() < 10
   {
      std::thread::yield_now();
   }
   let el = start.elapsed();
   let rx = fyro_db::handler::conn::get_raw_arrival_bytes();
   let rate = total_ops as f64 / el.as_secs_f64() / 1e6;
   let gbs = (rx as f64 / (1024.0 * 1024.0 * 1024.0)) / el.as_secs_f64();
   let gbps = (rx as f64 * 8.0) / el.as_secs_f64() / 1e9;

   println!("  FyroDB Ingress to Parser Doorstep (Zero Parsing, Zero Store):");
   println!(
      "    Elapsed: {:?} | Throughput: {:.2}M requests/sec arrived at parser ({:.2} GB/s, {:.2} Gbps)\n",
      el, rate, gbs, gbps
   );

   fyro_db::handler::conn::set_raw_arrival_only(false);
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

   let workers = num_cpus::get();
   let total_ops = clients * ops_per_client;

   println!("==========================================================================");
   println!("        SCIENTIFIC NETWORK LAYER-BY-LAYER BOTTLENECK ANALYSIS             ");
   println!("==========================================================================");
   println!(
      "Configuration: Workers: {} | Clients: {} | Total Ops: 10,000,000",
      workers, clients
   );
   println!("==========================================================================\n");

   // SECTION 1: Pure Raw Kernel TCP
   println!("── BENCHMARK 1: Pure Raw Linux Kernel Loopback TCP (Zero Application) ───");
   run_raw_kernel_tcp(workers, clients, ops_per_client, pipe_size);

   // Start FyroDB server for layers 2 & 3
   let _server_guard;
   if port == 0 {
      let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
      port = probe.local_addr().unwrap().port();
      drop(probe);

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

   // SECTION 2: FyroDB Ingress (Arrive at Parser, Zero Parsing)
   println!("── BENCHMARK 2: FyroDB Ingress (Arrived at Parser Buffer, No Parsing) ───");
   run_fyrodb_arrival(port, clients, ops_per_client, pipe_size);

   // SECTION 3: FyroDB Protocol Stack (Full RESP Parser + Replies, Null Store)
   println!("── BENCHMARK 3: FyroDB TCP + Full RESP Parser + Frame Replies ───────────");
   fyro_db::handler::conn::set_null_store(true);
   fyro_db::handler::conn::set_raw_arrival_only(false);

   // Warmup
   let mut warmup = dial(port);
   warmup.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
   let mut ping_resp = [0u8; 7];
   warmup.read_exact(&mut ping_resp).unwrap();
   drop(warmup);

   // Pipeline-64 SET
   let mut conns = Vec::with_capacity(clients);
   for _ in 0..clients {
      conns.push(dial(port));
   }
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
   println!(
      "  Pipeline-64 SET: {:>8.2?}   {:>8.2}M ops/sec",
      seq_el, seq_rate
   );

   // Pipelined SET
   let mut conns = Vec::with_capacity(clients);
   for _ in 0..clients {
      conns.push(dial(port));
   }
   let start_set = Instant::now();
   std::thread::scope(|s| {
      for (id, stream) in conns.into_iter().enumerate() {
         s.spawn(move || {
            run_pipeline(
               stream,
               ops_per_client,
               pipe_size,
               5,
               true,
               id * ops_per_client,
            );
         });
      }
   });
   let set_el = start_set.elapsed();
   let set_rate = total_ops as f64 / set_el.as_secs_f64() / 1e6;
   println!(
      "  Pipelined SET:   {:>8.2?}   {:>8.2}M ops/sec",
      set_el, set_rate
   );

   // Pipelined GET
   let mut conns = Vec::with_capacity(clients);
   for _ in 0..clients {
      conns.push(dial(port));
   }
   let start_get = Instant::now();
   std::thread::scope(|s| {
      for (id, stream) in conns.into_iter().enumerate() {
         s.spawn(move || {
            run_pipeline(
               stream,
               ops_per_client,
               pipe_size,
               11,
               false,
               id * ops_per_client,
            );
         });
      }
   });
   let get_el = start_get.elapsed();
   let get_rate = total_ops as f64 / get_el.as_secs_f64() / 1e6;
   println!(
      "  Pipelined GET:   {:>8.2?}   {:>8.2}M ops/sec",
      get_el, get_rate
   );

   println!("\n==========================================================================");
   println!("                         SUMMARY COMPARISON                               ");
   println!("==========================================================================");
   println!("  1. Raw Linux Kernel TCP loopback ceiling");
   println!("  2. FyroDB Mio Worker Ingress (ready at parser buffer, zero parsing)");
   println!("  3. FyroDB Full RESP Parser Protocol Layer (zero storage overhead)");
   println!("==========================================================================\n");
}
