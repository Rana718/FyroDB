//! End-to-end tests for dispatch-level `SET k v` + `EXPIRE k t` pair
//! coalescing. A pipelined cache-TTL pattern executes as one atomic
//! set-with-TTL under one entry lock, with byte-identical replies
//! (`+OK` then `:1`) and correct TTL persistence. All non-matching
//! sequences (lone SET, different-key EXPIRE, invalid TTL, arity changes)
//! fall through to normal dispatch with identical behavior.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

fn spawn_server() -> u16 {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    // Unlimited key capacity (usize::MAX): the alignment test creates
    // 100+ keys.
    let store = Arc::new(fyro_db::storage::store::Store::with_config(
        2,
        usize::MAX,
    ));
    let pubsub = Arc::new(fyro_db::pubsub::PubSub::new());
    for idx in 0..2 {
        let store = Arc::clone(&store);
        let pubsub = Arc::clone(&pubsub);
        std::thread::Builder::new()
            .name(format!("pair-test-worker-{idx}"))
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

fn cmd(parts: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n{}\r\n", p.len(), p).as_bytes());
    }
    out
}

#[test]
fn pair_replies_and_ttl_are_correct() {
    let port = spawn_server();
    let (mut s, mut r) = conn(port);

    // Pipelined pair: coalesced into one atomic set-with-TTL.
    let mut wire = cmd(&["SET", "pk", "v"]);
    wire.extend_from_slice(&cmd(&["EXPIRE", "pk", "60"]));
    s.write_all(&wire).unwrap();
    assert_eq!(read_reply(&mut r), b"+OK\r\n");
    assert_eq!(read_reply(&mut r), b":1\r\n");

    // The TTL must actually be attached to the entry.
    s.write_all(&cmd(&["TTL", "pk"])).unwrap();
    let ttl = read_reply(&mut r);
    assert_eq!(ttl[0], b':');
    let secs: i64 = std::str::from_utf8(&ttl[1..]).unwrap().trim().parse().unwrap();
    assert!((55..=60).contains(&secs), "TTL not persisted: {secs}");

    // The value must be readable.
    s.write_all(&cmd(&["GET", "pk"])).unwrap();
    assert_eq!(read_reply(&mut r), b"$1\r\nv\r\n");
}

#[test]
fn pipelined_pairs_stream_keeps_reply_alignment() {
    let port = spawn_server();
    let (mut s, mut r) = conn(port);

    // A burst of pairs, exactly the cache-TTL bench pattern: replies must
    // interleave +OK/:1 with perfect alignment.
    let mut wire = Vec::new();
    for i in 0..100 {
        let key = format!("p{i}");
        wire.extend_from_slice(&cmd(&["SET", &key, "val"]));
        wire.extend_from_slice(&cmd(&["EXPIRE", &key, "60"]));
    }
    s.write_all(&wire).unwrap();
    for i in 0..100 {
        assert_eq!(read_reply(&mut r), b"+OK\r\n", "pair {i} SET reply");
        assert_eq!(read_reply(&mut r), b":1\r\n", "pair {i} EXPIRE reply");
    }

    // Spot-check TTLs across the batch.
    for i in [0, 50, 99] {
        let key = format!("p{i}");
        s.write_all(&cmd(&["TTL", &key])).unwrap();
        let ttl = read_reply(&mut r);
        let secs: i64 = std::str::from_utf8(&ttl[1..]).unwrap().trim().parse().unwrap();
        assert!((55..=60).contains(&secs));
    }
}

#[test]
fn lone_set_and_mismatched_expire_fall_through() {
    let port = spawn_server();
    let (mut s, mut r) = conn(port);

    // Lone SET (nothing follows): normal dispatch.
    s.write_all(&cmd(&["SET", "sk", "v"])).unwrap();
    assert_eq!(read_reply(&mut r), b"+OK\r\n");

    // SET followed by EXPIRE of a DIFFERENT key: both dispatch normally;
    // the second command's reply reflects the other key (:0, missing).
    let mut wire = cmd(&["SET", "sk", "v2"]);
    wire.extend_from_slice(&cmd(&["EXPIRE", "other", "60"]));
    s.write_all(&wire).unwrap();
    assert_eq!(read_reply(&mut r), b"+OK\r\n");
    assert_eq!(read_reply(&mut r), b":0\r\n");

    // sk has no TTL.
    s.write_all(&cmd(&["TTL", "sk"])).unwrap();
    assert_eq!(read_reply(&mut r), b":-1\r\n");
}

#[test]
fn invalid_ttl_pair_replies_match_generic_path() {
    let port = spawn_server();
    let (mut s, mut r) = conn(port);

    let mut wire = cmd(&["SET", "bk", "v"]);
    wire.extend_from_slice(&cmd(&["EXPIRE", "bk", "notanumber"]));
    s.write_all(&wire).unwrap();
    assert_eq!(read_reply(&mut r), b"+OK\r\n");
    let err = read_reply(&mut r);
    assert!(
        err.starts_with(b"-ERR invalid expire time"),
        "unexpected reply: {err:?}"
    );

    // Connection survives; subsequent commands work.
    s.write_all(&cmd(&["PING"])).unwrap();
    assert_eq!(read_reply(&mut r), b"+PONG\r\n");

    // The SET half took effect without a TTL.
    s.write_all(&cmd(&["TTL", "bk"])).unwrap();
    assert_eq!(read_reply(&mut r), b":-1\r\n");
}

#[test]
fn set_with_extra_args_not_coalesced() {
    let port = spawn_server();
    let (mut s, mut r) = conn(port);

    // 4-arg SET (GET/XX/etc options) must not enter the pair path.
    let mut wire = cmd(&["SET", "optk", "v", "XX"]);
    wire.extend_from_slice(&cmd(&["EXPIRE", "optk", "60"]));
    s.write_all(&wire).unwrap();
    // SET XX on a missing key is a no-op (nil reply from the options path),
    // so the following EXPIRE correctly reports :0 — the pair path must not
    // have force-created the key.
    let first = read_reply(&mut r);
    assert!(!first.is_empty(), "SET reply: {first:?}");
    assert_eq!(read_reply(&mut r), b":0\r\n");
}

// Workers block in poll(); they exit with the test process (same pattern
// as the queue-coalescing and pubsub e2e tests).
