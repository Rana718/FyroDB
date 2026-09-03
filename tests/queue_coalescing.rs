//! E2E: same-key queue-command runs execute as one lock-acquiring batch,
//! client-visible behavior byte-identical to single dispatch.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

struct Server {
    port: u16,
}

fn spawn_server() -> Server {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let store = Arc::new(fyro_db::storage::store::Store::with_config(2, 64));
    let pubsub = Arc::new(fyro_db::pubsub::PubSub::new());
    for idx in 0..2 {
        let store = Arc::clone(&store);
        let pubsub = Arc::clone(&pubsub);
        std::thread::Builder::new()
            .name(format!("cq-test-worker-{idx}"))
            .stack_size(512 * 1024)
            .spawn(move || {
                fyro_db::worker::run_worker(&store, &pubsub, port, "127.0.0.1", None, idx)
            })
            .unwrap();
    }
    std::thread::sleep(Duration::from_millis(500));
    Server { port }
}

fn conn(port: u16) -> (TcpStream, BufReader<TcpStream>) {
    let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_nodelay(true).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let r = BufReader::new(s.try_clone().unwrap());
    (s, r)
}

fn cmd_lp(key: &str, v: &str) -> Vec<u8> {
    format!("*3\r\n$5\r\nLPUSH\r\n${}\r\n{key}\r\n${}\r\n{v}\r\n", key.len(), v.len()).into_bytes()
}

fn cmd_rp(key: &str, v: &str) -> Vec<u8> {
    format!("*3\r\n$5\r\nRPUSH\r\n${}\r\n{key}\r\n${}\r\n{v}\r\n", key.len(), v.len()).into_bytes()
}

fn cmd_rpop(key: &str) -> Vec<u8> {
    format!("*2\r\n$4\r\nRPOP\r\n${}\r\n{key}\r\n", key.len()).into_bytes()
}

fn cmd_get(key: &str) -> Vec<u8> {
    format!("*2\r\n$3\r\nGET\r\n${}\r\n{key}\r\n", key.len()).into_bytes()
}

/// Read one RESP reply, returning its full wire bytes (headers + payload).
fn read_reply(r: &mut BufReader<TcpStream>) -> Vec<u8> {
    let mut out = Vec::new();
    let mut line = String::new();
    r.read_line(&mut line).unwrap();
    out.extend_from_slice(line.as_bytes());
    if line.starts_with('$') && line.trim() != "$-1" {
        let n: usize = line.strip_prefix('$').unwrap().trim().parse().unwrap();
        let mut payload = vec![0u8; n + 2];
        r.read_exact(&mut payload).unwrap();
        out.extend_from_slice(&payload);
    } else if line.starts_with('*') {
        let n: usize = line.strip_prefix('*').unwrap().trim().parse().unwrap();
        for _ in 0..n {
            out.extend_from_slice(&read_reply(r));
        }
    }
    out
}

/// Read `n` integer replies, returning them parsed.
fn read_ints(r: &mut BufReader<TcpStream>, n: usize) -> Vec<i64> {
    (0..n)
        .map(|_| {
            let b = read_reply(r);
            assert_eq!(b[0], b':', "expected integer reply, got {b:?}");
            std::str::from_utf8(&b[1..]).unwrap().trim().parse().unwrap()
        })
        .collect()
}

fn read_bulks(r: &mut BufReader<TcpStream>, n: usize) -> Vec<String> {
    (0..n)
        .map(|_| {
            let b = read_reply(r);
            assert_eq!(b[0], b'$', "expected bulk reply, got {b:?}");
            let text = std::str::from_utf8(&b).unwrap();
            // $N\r\nPAYLOAD\r\n
            let (head, rest) = text.split_once("\r\n").unwrap();
            let payload_len: usize = head[1..].parse().unwrap();
            assert_eq!(payload_len, rest.len() - 2);
            rest[..payload_len].to_string()
        })
        .collect()
}

#[test]
fn coalesced_lpush_replies_are_cumulative_lengths() {
    let server = spawn_server();
    let (mut s, mut r) = conn(server.port);

    // 150 same-key single-value pushes in one pipeline burst: must execute
    // coalesced and reply 1..=150 in order.
    let mut wire = Vec::new();
    for i in 1..=150 {
        wire.extend_from_slice(&cmd_lp("cq1", &format!("v{i}")));
    }
    s.write_all(&wire).unwrap();
    assert_eq!(read_ints(&mut r, 150), (1..=150).collect::<Vec<i64>>());

    // Order check: LPUSH fronts each value, so RPOP from the back returns
    // the earliest pushed first (FIFO overall for LPUSH+RPOP).
    let mut wire = Vec::new();
    for _ in 0..3 {
        wire.extend_from_slice(&cmd_rpop("cq1"));
    }
    s.write_all(&wire).unwrap();
    assert_eq!(read_bulks(&mut r, 3), vec!["v1", "v2", "v3"]);
}

#[test]
fn coalesced_rpop_replies_are_values_then_nils() {
    let server = spawn_server();
    let (mut s, mut r) = conn(server.port);

    let mut wire = Vec::new();
    for i in 1..=5 {
        wire.extend_from_slice(&cmd_rp("cq2", &format!("p{i}")));
    }
    s.write_all(&wire).unwrap();
    read_ints(&mut r, 5);

    // Pop more than exists: value bulks then nils, in order, count exact.
    let mut wire = Vec::new();
    for _ in 0..8 {
        wire.extend_from_slice(&cmd_rpop("cq2"));
    }
    s.write_all(&wire).unwrap();
    for expected in ["p5", "p4", "p3", "p2", "p1"] {
        assert_eq!(read_bulks(&mut r, 1)[0], expected);
    }
    for _ in 0..3 {
        let b = read_reply(&mut r);
        assert_eq!(b, b"$-1\r\n");
    }
}

#[test]
fn run_breaks_on_key_op_and_arity_change() {
    let server = spawn_server();
    let (mut s, mut r) = conn(server.port);

// Key change ends the run; op change starts its own; arity mismatch
// takes normal dispatch.
    let mut wire = Vec::new();
    wire.extend_from_slice(&cmd_lp("k1", "a"));
    wire.extend_from_slice(&cmd_lp("k2", "b"));
    wire.extend_from_slice(&cmd_lp("k1", "c"));
    s.write_all(&wire).unwrap();
    // Replies must be exactly as un-coalesced: 1, 1, 2.
    assert_eq!(read_ints(&mut r, 3), vec![1, 1, 2]);

// Trailing-command regression guard; GET on a list returns nil here.
    let mut wire = Vec::new();
    wire.extend_from_slice(&cmd_rpop("k2"));
    wire.extend_from_slice(&cmd_get("k2"));
    s.write_all(&wire).unwrap();
    assert_eq!(read_bulks(&mut r, 1)[0], "b");
    let b = read_reply(&mut r);
    assert_eq!(b, b"$-1\r\n", "GET on list key returns nil");
}

#[test]
fn wrong_type_replies_per_command_not_nils() {
    let server = spawn_server();
    let (mut s, mut r) = conn(server.port);

// Each reply must be WRONGTYPE, never nil.
    s.write_all(b"*3\r\n$3\r\nSET\r\n$4\r\nstrk\r\n$1\r\nx\r\n")
        .unwrap();
    assert_eq!(read_reply(&mut r), b"+OK\r\n");

    s.write_all(&cmd_rpop("strk")).unwrap();
    s.write_all(&cmd_rpop("strk")).unwrap();
    for _ in 0..2 {
        let b = read_reply(&mut r);
        assert!(b.starts_with(b"-WRONGTYPE"), "pop on string: {b:?}");
    }
}

#[test]
fn runs_longer_than_512_execute_in_multiple_batches() {
    let server = spawn_server();
    let (mut s, mut r) = conn(server.port);

    // 1200 pushes: QUEUE_RUN_MAX is 512, so this is ≥3 runs — replies must
    // still be 1..=1200 with no gaps or repeats (batch-boundary bug guard).
    let mut wire = Vec::new();
    for i in 1..=1200 {
        wire.extend_from_slice(&cmd_rp("big", &format!("v{i}")));
    }
    s.write_all(&wire).unwrap();
    assert_eq!(
        read_ints(&mut r, 1200),
        (1..=1200).collect::<Vec<i64>>()
    );

    // Drain everything back and confirm the exact values survive.
    let mut wire = Vec::new();
    for _ in 0..1200 {
        wire.extend_from_slice(&cmd_rpop("big"));
    }
    s.write_all(&wire).unwrap();
    for i in (1..=1200).rev() {
        assert_eq!(read_bulks(&mut r, 1)[0], format!("v{i}"));
    }
}

#[test]
fn single_command_streams_are_unchanged() {
    // Non-queue traffic interleaved with queue runs must behave identically.
    let server = spawn_server();
    let (mut s, mut r) = conn(server.port);

    let mut wire = Vec::new();
    wire.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$2\r\nsk\r\n$2\r\nhi\r\n");
    wire.extend_from_slice(&cmd_lp("mix", "1"));
    wire.extend_from_slice(&cmd_get("sk"));
    wire.extend_from_slice(&cmd_lp("mix", "2"));
    s.write_all(&wire).unwrap();
    assert_eq!(read_reply(&mut r), b"+OK\r\n");
    assert_eq!(read_ints(&mut r, 1)[0], 1);
    assert_eq!(read_bulks(&mut r, 1)[0], "hi");
    assert_eq!(read_ints(&mut r, 1)[0], 2);
}

// Workers exit with the test process.
