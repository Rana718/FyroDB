use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Instant;

use fyro_db::pubsub::PubSub;
use fyro_db::storage::store::Store;
use fyro_db::worker::run_worker;

const NUM_WORKERS: usize = 12;
const NUM_CLIENTS: usize = 24;
const REQUESTS_PER_CLIENT: usize = 64_000;
const PIPELINE_DEPTH: usize = 64;

#[test]
fn bench_worker_network_throughput_and_slot_latency() {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let store = Arc::new(Store::with_config(32, 2_000_000));
    let pubsub = Arc::new(PubSub::new());

    // Spawn 8 Worker Threads using SO_REUSEPORT across 32 CPU cores
    for idx in 0..NUM_WORKERS {
        let store_w = Arc::clone(&store);
        let pubsub_w = Arc::clone(&pubsub);
        std::thread::Builder::new()
            .name(format!("worker-{}", idx))
            .spawn(move || {
                run_worker(&store_w, &pubsub_w, port, "127.0.0.1", None, idx);
            })
            .unwrap();
    }

    std::thread::sleep(std::time::Duration::from_millis(600));

    let total_ops = NUM_CLIENTS * REQUESTS_PER_CLIENT;

    println!("\n==========================================================================");
    println!("        TARGET: 10,000,000+ QPS MULTI-WORKER BENCHMARK");
    println!("   Workers: {} | Clients: {} | Req/Client: {} | Total Ops: {} | Pipeline: {}", 
             NUM_WORKERS, NUM_CLIENTS, REQUESTS_PER_CLIENT, total_ops, PIPELINE_DEPTH);
    println!("==========================================================================\n");

    // Warm-up & Pre-populate 5,000 keys
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

    // Benchmark 1: Pipelined GET (Targeting 10M+ QPS)
    let start_get = Instant::now();
    let mut handles = Vec::with_capacity(NUM_CLIENTS);

    for client_id in 0..NUM_CLIENTS {
        handles.push(std::thread::spawn(move || {
            let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect client");
            stream.set_nodelay(true).unwrap();
            let mut reader = BufReader::with_capacity(65536, stream.try_clone().unwrap());

            let mut batch_buf = Vec::with_capacity(PIPELINE_DEPTH * 48);
            let batches = REQUESTS_PER_CLIENT / PIPELINE_DEPTH;

            for b in 0..batches {
                batch_buf.clear();
                for i in 0..PIPELINE_DEPTH {
                    let k = (client_id * 50 + (b * PIPELINE_DEPTH + i)) % 1000;
                    let cmd = format!("*2\r\n$3\r\nGET\r\n$9\r\nperf:{:04}\r\n", k);
                    batch_buf.extend_from_slice(cmd.as_bytes());
                }
                stream.write_all(&batch_buf).unwrap();

                for _ in 0..PIPELINE_DEPTH {
                    let mut l1 = String::new();
                    let mut l2 = String::new();
                    reader.read_line(&mut l1).unwrap();
                    reader.read_line(&mut l2).unwrap();
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let get_elapsed = start_get.elapsed();
    let get_qps = (total_ops as f64) / get_elapsed.as_secs_f64();
    let get_us_per_req = (get_elapsed.as_micros() as f64) / (total_ops as f64);

    // Benchmark 2: Pipelined SET (Targeting 10M+ QPS)
    let start_set = Instant::now();
    let mut handles = Vec::with_capacity(NUM_CLIENTS);

    for client_id in 0..NUM_CLIENTS {
        handles.push(std::thread::spawn(move || {
            let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect client");
            stream.set_nodelay(true).unwrap();
            let mut reader = BufReader::with_capacity(65536, stream.try_clone().unwrap());

            let mut batch_buf = Vec::with_capacity(PIPELINE_DEPTH * 64);
            let batches = REQUESTS_PER_CLIENT / PIPELINE_DEPTH;

            for b in 0..batches {
                batch_buf.clear();
                for i in 0..PIPELINE_DEPTH {
                    let k = (client_id * 50 + (b * PIPELINE_DEPTH + i)) % 1000;
                    let cmd = format!("*3\r\n$3\r\nSET\r\n$9\r\nperf:{:04}\r\n$8\r\nnewvalue\r\n", k);
                    batch_buf.extend_from_slice(cmd.as_bytes());
                }
                stream.write_all(&batch_buf).unwrap();

                for _ in 0..PIPELINE_DEPTH {
                    let mut l = String::new();
                    reader.read_line(&mut l).unwrap();
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let set_elapsed = start_set.elapsed();
    let set_qps = (total_ops as f64) / set_elapsed.as_secs_f64();
    let set_us_per_req = (set_elapsed.as_micros() as f64) / (total_ops as f64);

    println!("--------------------------------------------------------------------------");
    println!("Operation        | Total Time  | Throughput (QPS / RPS) | Latency per Req");
    println!("--------------------------------------------------------------------------");
    println!("Pipelined GET    | {:>9.2?}   | {:>15.2} req/sec | {:>10.3} µs", get_elapsed, get_qps, get_us_per_req);
    println!("Pipelined SET    | {:>9.2?}   | {:>15.2} req/sec | {:>10.3} µs", set_elapsed, set_qps, set_us_per_req);
    println!("==========================================================================\n");

    let m_get = get_qps / 1_000_000.0;
    let m_set = set_qps / 1_000_000.0;
    println!("🔥 FINAL SCORE: GET: {:.2} Million QPS | SET: {:.2} Million QPS\n", m_get, m_set);
}
