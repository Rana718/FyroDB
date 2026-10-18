use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Instant;

use fyro_db::pubsub::PubSub;
use fyro_db::storage::store::Store;
use fyro_db::worker::run_worker;

#[test]
fn bench_2_workers_max_qps() {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let store = Arc::new(Store::with_config(32, 2_000_000));
    let pubsub = Arc::new(PubSub::new());

    // Exactly 2 Workers as requested!
    for idx in 0..2 {
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

    // Warm-up & Pre-populate keys
    let mut warmup = TcpStream::connect(("127.0.0.1", port)).expect("connect warmup");
    warmup.set_nodelay(true).unwrap();
    let mut warmup_r = BufReader::new(warmup.try_clone().unwrap());
    for i in 0..1000 {
        let cmd = format!("*3\r\n$3\r\nSET\r\n$9\r\nperf:{:04}\r\n$16\r\n0123456789abcdef\r\n", i);
        warmup.write_all(cmd.as_bytes()).unwrap();
        let mut line = String::new();
        warmup_r.read_line(&mut line).unwrap();
        assert_eq!(line.trim_end(), "+OK");
    }
    drop(warmup);

    for &(clients, pipeline) in &[
        (16, 256),
        (24, 512),
        (32, 512),
    ] {
        let total_ops = 1_536_000;
        let req_per_client = total_ops / clients;

        // 1. GET Benchmark with pre-baked batch and read_exact
        let start = Instant::now();
        let mut handles = Vec::with_capacity(clients);

        for client_id in 0..clients {
            handles.push(std::thread::spawn(move || {
                let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect client");
                stream.set_nodelay(true).unwrap();

                // Pre-bake request batch once
                let mut batch_buf = Vec::with_capacity(pipeline * 27);
                for i in 0..pipeline {
                    let k = (client_id * 50 + i) % 1000;
                    let cmd = format!("*2\r\n$3\r\nGET\r\n$9\r\nperf:{:04}\r\n", k);
                    batch_buf.extend_from_slice(cmd.as_bytes());
                }

                // Response size for GET: "$16\r\n0123456789abcdef\r\n" = 23 bytes per command
                let expected_bytes = pipeline * 23;
                let mut resp_buf = vec![0u8; expected_bytes];
                let batches = req_per_client / pipeline;

                for _ in 0..batches {
                    stream.write_all(&batch_buf).unwrap();
                    std::io::Read::read_exact(&mut stream, &mut resp_buf).unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let elapsed = start.elapsed();
        let qps = (total_ops as f64) / elapsed.as_secs_f64();
        println!(
            "2 WORKERS GET | Clients: {:2} | Pipeline: {:3} | Elapsed: {:6.2}ms | QPS: {:10.2}",
            clients, pipeline, elapsed.as_secs_f64() * 1000.0, qps
        );

        // 2. SET Benchmark with pre-baked batch and read_exact
        let start_set = Instant::now();
        let mut handles_set = Vec::with_capacity(clients);
        for client_id in 0..clients {
            handles_set.push(std::thread::spawn(move || {
                let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect client");
                stream.set_nodelay(true).unwrap();

                // Pre-bake request batch once
                let mut batch_buf = Vec::with_capacity(pipeline * 49);
                for i in 0..pipeline {
                    let k = (client_id * 50 + i) % 1000;
                    let cmd = format!("*3\r\n$3\r\nSET\r\n$9\r\nperf:{:04}\r\n$16\r\n0123456789abcdef\r\n", k);
                    batch_buf.extend_from_slice(cmd.as_bytes());
                }

                // Response size for SET: "+OK\r\n" = 5 bytes per command
                let expected_bytes = pipeline * 5;
                let mut resp_buf = vec![0u8; expected_bytes];
                let batches = req_per_client / pipeline;

                for _ in 0..batches {
                    stream.write_all(&batch_buf).unwrap();
                    std::io::Read::read_exact(&mut stream, &mut resp_buf).unwrap();
                }
            }));
        }
        for h in handles_set {
            h.join().unwrap();
        }
        let elapsed_set = start_set.elapsed();
        let qps_set = (total_ops as f64) / elapsed_set.as_secs_f64();
        println!(
            "2 WORKERS SET | Clients: {:2} | Pipeline: {:3} | Elapsed: {:6.2}ms | QPS: {:10.2}",
            clients, pipeline, elapsed_set.as_secs_f64() * 1000.0, qps_set
        );
        println!("---------------------------------------------------------------------------------");
    }
}
