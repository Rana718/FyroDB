use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Instant;

use fyro_db::pubsub::PubSub;
use fyro_db::storage::store::Store;
use fyro_db::worker::run_worker;

#[test]
fn bench_4_workers_2_cores() {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let store = Arc::new(Store::with_config(32, 2_000_000));
    let pubsub = Arc::new(PubSub::new());

    // Exactly 4 Workers!
    for idx in 0..4 {
        let store_w = Arc::clone(&store);
        let pubsub_w = Arc::clone(&pubsub);
        std::thread::Builder::new()
            .name(format!("worker-{}", idx))
            .spawn(move || {
                run_worker(&store_w, &pubsub_w, port, "127.0.0.1", None, idx);
            })
            .unwrap();
    }

    std::thread::sleep(std::time::Duration::from_millis(500));

    // Warm-up & Pre-populate 50,000 keys so all client keys exist in memory
    let mut warmup = TcpStream::connect(("127.0.0.1", port)).expect("connect warmup");
    warmup.set_nodelay(true).unwrap();
    let mut batch = Vec::with_capacity(1000 * 130);
    for i in 0..50 {
        batch.clear();
        for j in 0..1000 {
            let k = i * 1000 + j;
            let cmd = format!("*3\r\n$3\r\nSET\r\n$13\r\nperf:{:08}\r\n$16\r\n0123456789abcdef\r\n", k);
            batch.extend_from_slice(cmd.as_bytes());
            let hcmd = format!("*4\r\n$4\r\nHSET\r\n$14\r\nhperf:{:08}\r\n$6\r\nfield1\r\n$16\r\n0123456789abcdef\r\n", k);
            batch.extend_from_slice(hcmd.as_bytes());
        }
        warmup.write_all(&batch).unwrap();
        let mut resp = [0u8; 9000]; // 5000 for +OK\r\n and 4000 for :1\r\n
        warmup.read_exact(&mut resp).unwrap();
    }
    drop(warmup);

    println!("\n==========================================================================");
    println!("        🔥 4 WORKERS (2 CORES) - ACTUAL DATABASE SERVER BENCHMARK 🔥");
    println!("==========================================================================");

    for &(clients, pipeline) in &[
        (32, 512),
        (32, 1024),
        (36, 1024),
        (48, 1024),
        (64, 1024),
        (48, 2048),
    ] {
        let total_ops = 1_536_000;
        let req_per_client = total_ops / clients;

        // 1. GET Benchmark
        let start_get = Instant::now();
        let mut handles_get = Vec::with_capacity(clients);
        for client_id in 0..clients {
            handles_get.push(std::thread::spawn(move || {
                let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect client");
                stream.set_nodelay(true).unwrap();

                let mut batch_buf = Vec::with_capacity(pipeline * 31);
                for i in 0..pipeline {
                    let k = client_id * 1000 + (i % 1000);
                    let cmd = format!("*2\r\n$3\r\nGET\r\n$13\r\nperf:{:08}\r\n", k);
                    batch_buf.extend_from_slice(cmd.as_bytes());
                }

                let expected_bytes = pipeline * 23;
                let mut resp_buf = vec![0u8; expected_bytes];
                let batches = req_per_client / pipeline;

                for _ in 0..batches {
                    stream.write_all(&batch_buf).unwrap();
                    stream.read_exact(&mut resp_buf).unwrap();
                }
            }));
        }
        for h in handles_get {
            h.join().unwrap();
        }
        let elapsed_get = start_get.elapsed();
        let qps_get = (total_ops as f64) / elapsed_get.as_secs_f64();

        // 2. SET Benchmark
        let start_set = Instant::now();
        let mut handles_set = Vec::with_capacity(clients);
        for client_id in 0..clients {
            handles_set.push(std::thread::spawn(move || {
                let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect client");
                stream.set_nodelay(true).unwrap();

                let mut batch_buf = Vec::with_capacity(pipeline * 53);
                for i in 0..pipeline {
                    let k = client_id * 1000 + (i % 1000);
                    let cmd = format!("*3\r\n$3\r\nSET\r\n$13\r\nperf:{:08}\r\n$16\r\n0123456789abcdef\r\n", k);
                    batch_buf.extend_from_slice(cmd.as_bytes());
                }

                let expected_bytes = pipeline * 5;
                let mut resp_buf = vec![0u8; expected_bytes];
                let batches = req_per_client / pipeline;

                for _ in 0..batches {
                    stream.write_all(&batch_buf).unwrap();
                    stream.read_exact(&mut resp_buf).unwrap();
                }
            }));
        }
        for h in handles_set {
            h.join().unwrap();
        }
        let elapsed_set = start_set.elapsed();
        let qps_set = (total_ops as f64) / elapsed_set.as_secs_f64();

        // 3. HGET Benchmark
        let start_hget = Instant::now();
        let mut handles_hget = Vec::with_capacity(clients);
        for client_id in 0..clients {
            handles_hget.push(std::thread::spawn(move || {
                let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect client");
                stream.set_nodelay(true).unwrap();

                let mut batch_buf = Vec::with_capacity(pipeline * 45);
                for i in 0..pipeline {
                    let k = client_id * 1000 + (i % 1000);
                    let cmd = format!("*3\r\n$4\r\nHGET\r\n$14\r\nhperf:{:08}\r\n$6\r\nfield1\r\n", k);
                    batch_buf.extend_from_slice(cmd.as_bytes());
                }

                let expected_bytes = pipeline * 23;
                let mut resp_buf = vec![0u8; expected_bytes];
                let batches = req_per_client / pipeline;

                for _ in 0..batches {
                    stream.write_all(&batch_buf).unwrap();
                    stream.read_exact(&mut resp_buf).unwrap();
                }
            }));
        }
        for h in handles_hget {
            h.join().unwrap();
        }
        let elapsed_hget = start_hget.elapsed();
        let qps_hget = (total_ops as f64) / elapsed_hget.as_secs_f64();

        // 4. HSET Benchmark
        let start_hset = Instant::now();
        let mut handles_hset = Vec::with_capacity(clients);
        for client_id in 0..clients {
            handles_hset.push(std::thread::spawn(move || {
                let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect client");
                stream.set_nodelay(true).unwrap();

                let mut batch_buf = Vec::with_capacity(pipeline * 67);
                for i in 0..pipeline {
                    let k = client_id * 1000 + (i % 1000);
                    let cmd = format!("*4\r\n$4\r\nHSET\r\n$14\r\nhperf:{:08}\r\n$6\r\nfield1\r\n$16\r\n0123456789abcdef\r\n", k);
                    batch_buf.extend_from_slice(cmd.as_bytes());
                }

                let expected_bytes = pipeline * 4;
                let mut resp_buf = vec![0u8; expected_bytes];
                let batches = req_per_client / pipeline;

                for _ in 0..batches {
                    stream.write_all(&batch_buf).unwrap();
                    stream.read_exact(&mut resp_buf).unwrap();
                }
            }));
        }
        for h in handles_hset {
            h.join().unwrap();
        }
        let elapsed_hset = start_hset.elapsed();
        let qps_hset = (total_ops as f64) / elapsed_hset.as_secs_f64();

        println!(
            "4 WORKERS | Clients: {:2} | Pipe: {:4} || GET: {:6.2}M | SET: {:6.2}M | HGET: {:6.2}M | HSET: {:6.2}M QPS",
            clients, pipeline,
            qps_get / 1_000_000.0,
            qps_set / 1_000_000.0,
            qps_hget / 1_000_000.0,
            qps_hset / 1_000_000.0,
        );
    }
    println!("==========================================================================\n");
}
