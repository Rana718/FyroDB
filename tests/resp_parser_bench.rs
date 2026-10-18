use std::time::Instant;
use fyro_db::utils::parser::{ParseResult, RespParser};

#[test]
fn bench_pure_resp_parser_throughput() {
    println!("\n==========================================================================");
    println!("          ⚡ FYRODB REDIS RESP PROTOCOL PARSER BENCHMARK ⚡");
    println!("==========================================================================\n");

    // 1. Prepare raw Redis RESP payloads
    let get_cmd = b"*2\r\n$3\r\nGET\r\n$9\r\nperf:0001\r\n";
    let set_cmd = b"*3\r\n$3\r\nSET\r\n$9\r\nperf:0001\r\n$16\r\n0123456789abcdef\r\n";
    let ping_cmd = b"*1\r\n$4\r\nPING\r\n";
    let del_cmd = b"*2\r\n$3\r\nDEL\r\n$9\r\nperf:0001\r\n";
    let hget_cmd = b"*3\r\n$4\r\nHGET\r\n$9\r\nperf:0001\r\n$6\r\nfield1\r\n";
    let hset_cmd = b"*4\r\n$4\r\nHSET\r\n$9\r\nperf:0001\r\n$6\r\nfield1\r\n$16\r\n0123456789abcdef\r\n";

    // Benchmark 0: Pure In-Memory Zero-Copy GET Parser (10 Million commands)
    {
        const NUM_COMMANDS: usize = 10_000_000;
        let mut big_buf = Vec::with_capacity(NUM_COMMANDS * get_cmd.len());
        for _ in 0..NUM_COMMANDS {
            big_buf.extend_from_slice(get_cmd);
        }

        let mut parser = RespParser::new();
        // Pre-fill the parser buffer with all 10M commands to test raw parsing latency
        let target = parser.read_buf();
        // Grow to accommodate
        let needed = big_buf.len();
        let current_cap = target.len();
        if current_cap < needed {
            // feed in chunks
        }

        // We can feed all bytes directly
        let mut offset = 0;
        while offset < big_buf.len() {
            let buf = parser.read_buf();
            let chunk = buf.len().min(big_buf.len() - offset);
            buf[..chunk].copy_from_slice(&big_buf[offset..offset + chunk]);
            parser.did_fill(chunk);
            offset += chunk;
        }

        let start = Instant::now();
        let mut parsed_count = 0usize;

        while let ParseResult::Complete = parser.parse_one() {
            parsed_count += 1;
        }

        let elapsed = start.elapsed();
        let qps = (parsed_count as f64) / elapsed.as_secs_f64();
        let ns_per_cmd = (elapsed.as_nanos() as f64) / (parsed_count as f64);

        println!("── 0. Pure Zero-Copy GET Parser (*2 $3 GET $9 key)");
        println!("   Commands Parsed : {} (10 Million)", parsed_count);
        println!("   Time Taken      : {:.2} ms", elapsed.as_secs_f64() * 1000.0);
        println!("   Parser Speed    : {:.2} MILLION commands/sec", qps / 1_000_000.0);
        println!("   Latency/Command : {:.2} ns / cmd\n", ns_per_cmd);
    }

    // Benchmark 1: Single-Thread Streaming GET Parser (*2 $3 GET $9 key)
    {
        const NUM_COMMANDS: usize = 10_000_000;
        const BATCH_SIZE: usize = 10_000;
        let mut batch_buf = Vec::with_capacity(BATCH_SIZE * get_cmd.len());
        for _ in 0..BATCH_SIZE {
            batch_buf.extend_from_slice(get_cmd);
        }

        let mut parser = RespParser::new();
        let mut parsed_count = 0usize;
        let start = Instant::now();

        while parsed_count < NUM_COMMANDS {
            let mut offset = 0;
            while offset < batch_buf.len() && parsed_count < NUM_COMMANDS {
                let buf = parser.read_buf();
                let n = buf.len().min(batch_buf.len() - offset);
                buf[..n].copy_from_slice(&batch_buf[offset..offset + n]);
                parser.did_fill(n);
                offset += n;

                while let ParseResult::Complete = parser.parse_one() {
                    parsed_count += 1;
                }
            }
        }

        let elapsed = start.elapsed();
        let qps = (parsed_count as f64) / elapsed.as_secs_f64();
        let ns_per_cmd = (elapsed.as_nanos() as f64) / (parsed_count as f64);

        println!("── 1. Single-Core Streaming GET Parser (*2 $3 GET $9 key)");
        println!("   Commands Parsed : {} (10 Million)", parsed_count);
        println!("   Time Taken      : {:.2} ms", elapsed.as_secs_f64() * 1000.0);
        println!("   Parser Speed    : {:.2} MILLION commands/sec", qps / 1_000_000.0);
        println!("   Latency/Command : {:.2} ns / cmd\n", ns_per_cmd);
    }

    // Benchmark 2: Single-Thread Streaming SET Parser (*3 $3 SET $9 key $16 val)
    {
        const NUM_COMMANDS: usize = 10_000_000;
        const BATCH_SIZE: usize = 10_000;
        let mut batch_buf = Vec::with_capacity(BATCH_SIZE * set_cmd.len());
        for _ in 0..BATCH_SIZE {
            batch_buf.extend_from_slice(set_cmd);
        }

        let mut parser = RespParser::new();
        let mut parsed_count = 0usize;
        let start = Instant::now();

        while parsed_count < NUM_COMMANDS {
            let mut offset = 0;
            while offset < batch_buf.len() && parsed_count < NUM_COMMANDS {
                let buf = parser.read_buf();
                let n = buf.len().min(batch_buf.len() - offset);
                buf[..n].copy_from_slice(&batch_buf[offset..offset + n]);
                parser.did_fill(n);
                offset += n;

                while let ParseResult::Complete = parser.parse_one() {
                    parsed_count += 1;
                }
            }
        }

        let elapsed = start.elapsed();
        let qps = (parsed_count as f64) / elapsed.as_secs_f64();
        let ns_per_cmd = (elapsed.as_nanos() as f64) / (parsed_count as f64);

        println!("── 2. Single-Core Streaming SET Parser (*3 $3 SET $9 key $16 val)");
        println!("   Commands Parsed : {} (10 Million)", parsed_count);
        println!("   Time Taken      : {:.2} ms", elapsed.as_secs_f64() * 1000.0);
        println!("   Parser Speed    : {:.2} MILLION commands/sec", qps / 1_000_000.0);
        println!("   Latency/Command : {:.2} ns / cmd\n", ns_per_cmd);
    }

    // Benchmark 3: Single-Thread Streaming PING Parser (*1 $4 PING)
    {
        const NUM_COMMANDS: usize = 10_000_000;
        const BATCH_SIZE: usize = 10_000;
        let mut batch_buf = Vec::with_capacity(BATCH_SIZE * ping_cmd.len());
        for _ in 0..BATCH_SIZE {
            batch_buf.extend_from_slice(ping_cmd);
        }

        let mut parser = RespParser::new();
        let mut parsed_count = 0usize;
        let start = Instant::now();

        while parsed_count < NUM_COMMANDS {
            let mut offset = 0;
            while offset < batch_buf.len() && parsed_count < NUM_COMMANDS {
                let buf = parser.read_buf();
                let n = buf.len().min(batch_buf.len() - offset);
                buf[..n].copy_from_slice(&batch_buf[offset..offset + n]);
                parser.did_fill(n);
                offset += n;

                while let ParseResult::Complete = parser.parse_one() {
                    parsed_count += 1;
                }
            }
        }

        let elapsed = start.elapsed();
        let qps = (parsed_count as f64) / elapsed.as_secs_f64();
        let ns_per_cmd = (elapsed.as_nanos() as f64) / (parsed_count as f64);

        println!("── 3. Single-Core Streaming PING Parser (*1 $4 PING)");
        println!("   Commands Parsed : {} (10 Million)", parsed_count);
        println!("   Time Taken      : {:.2} ms", elapsed.as_secs_f64() * 1000.0);
        println!("   Parser Speed    : {:.2} MILLION commands/sec", qps / 1_000_000.0);
        println!("   Latency/Command : {:.2} ns / cmd\n", ns_per_cmd);
    }

    // Benchmark 3b: Single-Thread Streaming DEL Parser (*2 $3 DEL $9 key)
    {
        const NUM_COMMANDS: usize = 10_000_000;
        const BATCH_SIZE: usize = 10_000;
        let mut batch_buf = Vec::with_capacity(BATCH_SIZE * del_cmd.len());
        for _ in 0..BATCH_SIZE {
            batch_buf.extend_from_slice(del_cmd);
        }

        let mut parser = RespParser::new();
        let mut parsed_count = 0usize;
        let start = Instant::now();

        while parsed_count < NUM_COMMANDS {
            let mut offset = 0;
            while offset < batch_buf.len() && parsed_count < NUM_COMMANDS {
                let buf = parser.read_buf();
                let n = buf.len().min(batch_buf.len() - offset);
                buf[..n].copy_from_slice(&batch_buf[offset..offset + n]);
                parser.did_fill(n);
                offset += n;

                while let ParseResult::Complete = parser.parse_one() {
                    parsed_count += 1;
                }
            }
        }

        let elapsed = start.elapsed();
        let qps = (parsed_count as f64) / elapsed.as_secs_f64();
        let ns_per_cmd = (elapsed.as_nanos() as f64) / (parsed_count as f64);

        println!("── 3b. Single-Core Streaming DEL Parser (*2 $3 DEL $9 key)");
        println!("   Commands Parsed : {} (10 Million)", parsed_count);
        println!("   Time Taken      : {:.2} ms", elapsed.as_secs_f64() * 1000.0);
        println!("   Parser Speed    : {:.2} MILLION commands/sec", qps / 1_000_000.0);
        println!("   Latency/Command : {:.2} ns / cmd\n", ns_per_cmd);
    }

    // Benchmark 3c: Single-Thread Streaming HGET Parser (*3 $4 HGET $9 key $6 field)
    {
        const NUM_COMMANDS: usize = 10_000_000;
        const BATCH_SIZE: usize = 10_000;
        let mut batch_buf = Vec::with_capacity(BATCH_SIZE * hget_cmd.len());
        for _ in 0..BATCH_SIZE {
            batch_buf.extend_from_slice(hget_cmd);
        }

        let mut parser = RespParser::new();
        let mut parsed_count = 0usize;
        let start = Instant::now();

        while parsed_count < NUM_COMMANDS {
            let mut offset = 0;
            while offset < batch_buf.len() && parsed_count < NUM_COMMANDS {
                let buf = parser.read_buf();
                let n = buf.len().min(batch_buf.len() - offset);
                buf[..n].copy_from_slice(&batch_buf[offset..offset + n]);
                parser.did_fill(n);
                offset += n;

                while let ParseResult::Complete = parser.parse_one() {
                    parsed_count += 1;
                }
            }
        }

        let elapsed = start.elapsed();
        let qps = (parsed_count as f64) / elapsed.as_secs_f64();
        let ns_per_cmd = (elapsed.as_nanos() as f64) / (parsed_count as f64);

        println!("── 3c. Single-Core Streaming HGET Parser (*3 $4 HGET $9 key $6 field)");
        println!("   Commands Parsed : {} (10 Million)", parsed_count);
        println!("   Time Taken      : {:.2} ms", elapsed.as_secs_f64() * 1000.0);
        println!("   Parser Speed    : {:.2} MILLION commands/sec", qps / 1_000_000.0);
        println!("   Latency/Command : {:.2} ns / cmd\n", ns_per_cmd);
    }

    // Benchmark 3d: Single-Thread Streaming HSET Parser (*4 $4 HSET $9 key $6 field $16 val)
    {
        const NUM_COMMANDS: usize = 10_000_000;
        const BATCH_SIZE: usize = 10_000;
        let mut batch_buf = Vec::with_capacity(BATCH_SIZE * hset_cmd.len());
        for _ in 0..BATCH_SIZE {
            batch_buf.extend_from_slice(hset_cmd);
        }

        let mut parser = RespParser::new();
        let mut parsed_count = 0usize;
        let start = Instant::now();

        while parsed_count < NUM_COMMANDS {
            let mut offset = 0;
            while offset < batch_buf.len() && parsed_count < NUM_COMMANDS {
                let buf = parser.read_buf();
                let n = buf.len().min(batch_buf.len() - offset);
                buf[..n].copy_from_slice(&batch_buf[offset..offset + n]);
                parser.did_fill(n);
                offset += n;

                while let ParseResult::Complete = parser.parse_one() {
                    parsed_count += 1;
                }
            }
        }

        let elapsed = start.elapsed();
        let qps = (parsed_count as f64) / elapsed.as_secs_f64();
        let ns_per_cmd = (elapsed.as_nanos() as f64) / (parsed_count as f64);

        println!("── 3d. Single-Core Streaming HSET Parser (*4 $4 HSET $9 key $6 field $16 val)");
        println!("   Commands Parsed : {} (10 Million)", parsed_count);
        println!("   Time Taken      : {:.2} ms", elapsed.as_secs_f64() * 1000.0);
        println!("   Parser Speed    : {:.2} MILLION commands/sec", qps / 1_000_000.0);
        println!("   Latency/Command : {:.2} ns / cmd\n", ns_per_cmd);
    }

    // Benchmark 4: Multi-Core Parser Throughput (2 Cores / 2 Threads)
    {
        const THREADS: usize = 2;
        const COMMANDS_PER_THREAD: usize = 10_000_000;
        const BATCH_SIZE: usize = 10_000;

        let start = Instant::now();
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                std::thread::spawn(move || {
                    let mut batch_buf = Vec::with_capacity(BATCH_SIZE * get_cmd.len());
                    for _ in 0..BATCH_SIZE {
                        batch_buf.extend_from_slice(get_cmd);
                    }
                    let mut parser = RespParser::new();
                    let mut count = 0;

                    while count < COMMANDS_PER_THREAD {
                        let mut offset = 0;
                        while offset < batch_buf.len() && count < COMMANDS_PER_THREAD {
                            let buf = parser.read_buf();
                            let n = buf.len().min(batch_buf.len() - offset);
                            buf[..n].copy_from_slice(&batch_buf[offset..offset + n]);
                            parser.did_fill(n);
                            offset += n;

                            while let ParseResult::Complete = parser.parse_one() {
                                count += 1;
                            }
                        }
                    }
                    count
                })
            })
            .collect();

        let mut total_parsed = 0;
        for h in handles {
            total_parsed += h.join().unwrap();
        }

        let elapsed = start.elapsed();
        let qps = (total_parsed as f64) / elapsed.as_secs_f64();

        println!("── 4. Two-Core Parallel Parser Throughput (GET)");
        println!("   Total Parsed    : {} (20 Million)", total_parsed);
        println!("   Time Taken      : {:.2} ms", elapsed.as_secs_f64() * 1000.0);
        println!("   Throughput      : {:.2} MILLION commands/sec\n", qps / 1_000_000.0);
    }

    // Benchmark 5: Multi-Core Parser Throughput (4 Cores / 4 Threads)
    {
        const THREADS: usize = 4;
        const COMMANDS_PER_THREAD: usize = 10_000_000;
        const BATCH_SIZE: usize = 10_000;

        let start = Instant::now();
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                std::thread::spawn(move || {
                    let mut batch_buf = Vec::with_capacity(BATCH_SIZE * get_cmd.len());
                    for _ in 0..BATCH_SIZE {
                        batch_buf.extend_from_slice(get_cmd);
                    }
                    let mut parser = RespParser::new();
                    let mut count = 0;

                    while count < COMMANDS_PER_THREAD {
                        let mut offset = 0;
                        while offset < batch_buf.len() && count < COMMANDS_PER_THREAD {
                            let buf = parser.read_buf();
                            let n = buf.len().min(batch_buf.len() - offset);
                            buf[..n].copy_from_slice(&batch_buf[offset..offset + n]);
                            parser.did_fill(n);
                            offset += n;

                            while let ParseResult::Complete = parser.parse_one() {
                                count += 1;
                            }
                        }
                    }
                    count
                })
            })
            .collect();

        let mut total_parsed = 0;
        for h in handles {
            total_parsed += h.join().unwrap();
        }

        let elapsed = start.elapsed();
        let qps = (total_parsed as f64) / elapsed.as_secs_f64();

        println!("── 5. Four-Core Parallel Parser Throughput (GET)");
        println!("   Total Parsed    : {} (40 Million)", total_parsed);
        println!("   Time Taken      : {:.2} ms", elapsed.as_secs_f64() * 1000.0);
        println!("   Throughput      : {:.2} MILLION commands/sec\n", qps / 1_000_000.0);
    }

    println!("==========================================================================\n");
}
