//! E2E: same-key write runs execute under one lock with byte-identical
//! replies.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

fn spawn_server() -> u16 {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let store = Arc::new(fyro_db::storage::store::Store::with_config(2, usize::MAX));
    let pubsub = Arc::new(fyro_db::pubsub::PubSub::new());
    for idx in 0..2 {
        let store = Arc::clone(&store);
        let pubsub = Arc::clone(&pubsub);
        std::thread::Builder::new()
            .name(format!("wr-test-worker-{idx}"))
            .stack_size(512 * 1024)
            .spawn(move || {
                fyro_db::worker::run_worker(&store, &pubsub, port, "127.0.0.1", None, idx)
            })
            .unwrap();
    }
    std::thread::sleep(Duration::from_millis(500));
    port
}

fn conn(port: u16) -> (TcpStream, BufReader<TcpStream>) {
    let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_nodelay(true).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let r = BufReader::new(s.try_clone().unwrap());
    (s, r)
}

fn cmd(parts: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n{}\r\n", p.len(), p).as_bytes());
    }
    out
}

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

fn read_ints(r: &mut BufReader<TcpStream>, n: usize) -> Vec<i64> {
    (0..n)
        .map(|_| {
            let b = read_reply(r);
            assert_eq!(b[0], b':', "expected integer, got {b:?}");
            std::str::from_utf8(&b[1..]).unwrap().trim().parse().unwrap()
        })
        .collect()
}

#[test]
fn set_run_last_value_wins_all_ok() {
    let port = spawn_server();
    let (mut s, mut r) = conn(port);

    let mut wire = Vec::new();
    for i in 0..100 {
        wire.extend_from_slice(&cmd(&["SET", "sk", &format!("v{i}")]));
    }
    s.write_all(&wire).unwrap();
    for _ in 0..100 {
        assert_eq!(read_reply(&mut r), b"+OK\r\n");
    }
    s.write_all(&cmd(&["GET", "sk"])).unwrap();
    assert_eq!(read_reply(&mut r), b"$3\r\nv99\r\n");

    // TTL is cleared by plain SET runs.
    s.write_all(&cmd(&["EXPIRE", "sk", "60"])).unwrap();
    assert_eq!(read_reply(&mut r), b":1\r\n");
    let mut wire = Vec::new();
    wire.extend_from_slice(&cmd(&["SET", "sk", "fresh"]));
    wire.extend_from_slice(&cmd(&["SET", "sk", "fresh2"]));
    s.write_all(&wire).unwrap();
    assert_eq!(read_reply(&mut r), b"+OK\r\n");
    assert_eq!(read_reply(&mut r), b"+OK\r\n");
    s.write_all(&cmd(&["TTL", "sk"])).unwrap();
    assert_eq!(read_reply(&mut r), b":-1\r\n");
}

#[test]
fn incr_run_replies_are_consecutive_values() {
    let port = spawn_server();
    let (mut s, mut r) = conn(port);

    let mut wire = Vec::new();
    for _ in 0..150 {
        wire.extend_from_slice(&cmd(&["INCR", "ctr"]));
    }
    s.write_all(&wire).unwrap();
    assert_eq!(read_ints(&mut r, 150), (1..=150).collect::<Vec<i64>>());

    // A second run continues from the persisted value.
    let mut wire = Vec::new();
    for _ in 0..10 {
        wire.extend_from_slice(&cmd(&["INCR", "ctr"]));
    }
    s.write_all(&wire).unwrap();
    assert_eq!(read_ints(&mut r, 10), (151..=160).collect::<Vec<i64>>());
}

#[test]
fn hset_run_first_field_adds_rest_overwrite() {
    let port = spawn_server();
    let (mut s, mut r) = conn(port);

    let mut wire = Vec::new();
    for i in 0..100 {
        wire.extend_from_slice(&cmd(&["HSET", "hk", "f", &format!("v{i}")]));
    }
    s.write_all(&wire).unwrap();
    // First command adds the field; the rest overwrite it.
    let replies = read_ints(&mut r, 100);
    assert_eq!(replies[0], 1);
    assert!(replies[1..].iter().all(|&x| x == 0));

    s.write_all(&cmd(&["HGET", "hk", "f"])).unwrap();
    assert_eq!(read_reply(&mut r), b"$3\r\nv99\r\n");

    // Distinct fields all add.
    let mut wire = Vec::new();
    for i in 0..5 {
        wire.extend_from_slice(&cmd(&["HSET", "hk", &format!("g{i}"), "x"]));
    }
    s.write_all(&wire).unwrap();
    assert_eq!(read_ints(&mut r, 5), vec![1, 1, 1, 1, 1]);
}

#[test]
fn sadd_run_counts_new_members() {
    let port = spawn_server();
    let (mut s, mut r) = conn(port);

    let mut wire = Vec::new();
    for i in 0..100 {
        wire.extend_from_slice(&cmd(&["SADD", "st", &format!("m{i}")]));
    }
    s.write_all(&wire).unwrap();
    assert_eq!(read_ints(&mut r, 100), vec![1; 100]);

    // Re-adding the same members reports zero.
    let mut wire = Vec::new();
    for i in 0..10 {
        wire.extend_from_slice(&cmd(&["SADD", "st", &format!("m{i}")]));
    }
    s.write_all(&wire).unwrap();
    assert_eq!(read_ints(&mut r, 10), vec![0; 10]);
}

#[test]
fn zadd_run_counts_new_members_and_updates() {
    let port = spawn_server();
    let (mut s, mut r) = conn(port);

    let mut wire = Vec::new();
    for i in 0..100 {
        wire.extend_from_slice(&cmd(&["ZADD", "zk", &format!("{i}.5"), &format!("m{i}")]));
    }
    s.write_all(&wire).unwrap();
    assert_eq!(read_ints(&mut r, 100), vec![1; 100]);

// 9.5 is exactly representable (format_float round-trips cleanly).
    s.write_all(&cmd(&["ZADD", "zk", "9.5", "m0"])).unwrap();
    assert_eq!(read_ints(&mut r, 1), vec![0]);
    s.write_all(&cmd(&["ZSCORE", "zk", "m0"])).unwrap();
    assert_eq!(read_reply(&mut r), b"$3\r\n9.5\r\n");
}

#[test]
fn write_runs_break_on_key_and_op_changes() {
    let port = spawn_server();
    let (mut s, mut r) = conn(port);

    // SET k1, SET k2 (key change), INCR k1 (op change), trailing GET.
    let mut wire = cmd(&["SET", "k1", "a"]);
    wire.extend_from_slice(&cmd(&["SET", "k2", "b"]));
    wire.extend_from_slice(&cmd(&["INCR", "c1"]));
    wire.extend_from_slice(&cmd(&["GET", "c1"]));
    s.write_all(&wire).unwrap();
    assert_eq!(read_reply(&mut r), b"+OK\r\n");
    assert_eq!(read_reply(&mut r), b"+OK\r\n");
    assert_eq!(read_reply(&mut r), b":1\r\n");
    assert_eq!(read_reply(&mut r), b"$1\r\n1\r\n");

    // ZADD with flags (NX) must not coalesce with plain ZADDs.
    let mut wire = cmd(&["ZADD", "zk2", "1.0", "a"]);
    wire.extend_from_slice(&cmd(&["ZADD", "zk2", "NX", "2.0", "b"]));
    s.write_all(&wire).unwrap();
    assert_eq!(read_ints(&mut r, 1), vec![1]);
    // NX on a new member adds it.
    let reply = read_reply(&mut r);
    assert!(reply.first() == Some(&b':'), "NX zadd reply: {reply:?}");
}

#[test]
fn incr_wrong_type_errors_per_command() {
    let port = spawn_server();
    let (mut s, mut r) = conn(port);

    s.write_all(&cmd(&["SET", "strk", "notanumber"])).unwrap();
    assert_eq!(read_reply(&mut r), b"+OK\r\n");

    let mut wire = Vec::new();
    for _ in 0..5 {
        wire.extend_from_slice(&cmd(&["INCR", "strk"]));
    }
    s.write_all(&wire).unwrap();
    for _ in 0..5 {
        let b = read_reply(&mut r);
        assert!(
            b.starts_with(b"-ERR value is not an integer"),
            "incr on string: {b:?}"
        );
    }
}

#[test]
fn zadd_bad_score_fails_only_that_command() {
    let port = spawn_server();
    let (mut s, mut r) = conn(port);

    let mut wire = cmd(&["ZADD", "zk3", "1.0", "good"]);
    wire.extend_from_slice(&cmd(&["ZADD", "zk3", "banana", "bad"]));
    wire.extend_from_slice(&cmd(&["ZADD", "zk3", "2.0", "good2"]));
    s.write_all(&wire).unwrap();
    assert_eq!(read_ints(&mut r, 1), vec![1]);
    assert_eq!(
        read_reply(&mut r),
        b"-ERR value is not a float\r\n"
    );
    assert_eq!(read_ints(&mut r, 1), vec![1]);

    // Both good members landed.
    s.write_all(&cmd(&["ZCARD", "zk3"])).unwrap();
    assert_eq!(read_ints(&mut r, 1), vec![2]);
}

// Workers block in poll(); they exit with the test process.
