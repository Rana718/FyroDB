//! E2E: same-channel PUBLISH runs batch into one publish_batch while
//! replies and in-order delivery stay byte-identical.

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
            .name(format!("pub-test-worker-{idx}"))
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

/// Read one `message`-type delivery frame and return the payload.
fn read_delivery(r: &mut BufReader<TcpStream>) -> String {
    let frame = read_reply(r);
    let text = std::str::from_utf8(&frame).unwrap();
    // *3\r\n$7\r\nmessage\r\n$<n>\r\n<channel>\r\n$<m>\r\n<payload>\r\n
    let parts: Vec<&str> = text.split("\r\n").collect();
    // [array hdr, "$7", "message", "$n", channel, "$m", payload]
    parts[6].to_string()
}

#[test]
fn batched_publishes_deliver_in_order_to_all_subscribers() {
    let port = spawn_server();

    // Two subscribers.
    let mut subs = Vec::new();
    for _ in 0..2 {
        let (mut s, mut r) = conn(port);
        s.write_all(&cmd(&["SUBSCRIBE", "ch"])).unwrap();
        let _ = read_reply(&mut r); // confirmation
        subs.push((s, r));
    }

    // One publisher pipelines 200 same-channel publishes.
    let (mut p, mut pr) = conn(port);
    let mut wire = Vec::new();
    for i in 0..200 {
        wire.extend_from_slice(&cmd(&["PUBLISH", "ch", &format!("m{i}")]));
    }
    p.write_all(&wire).unwrap();

    // Every reply is the subscriber count.
    for _ in 0..200 {
        assert_eq!(read_reply(&mut pr), b":2\r\n");
    }

    // Both subscribers receive all 200 messages in publish order.
    for (_, r) in subs.iter_mut() {
        for i in 0..200 {
            assert_eq!(read_delivery(r), format!("m{i}"), "out of order at {i}");
        }
    }
}

#[test]
fn publish_run_breaks_on_channel_change_and_trailing_command() {
    let port = spawn_server();
    let (mut s, mut r) = conn(port);
    s.write_all(&cmd(&["SUBSCRIBE", "a"])).unwrap();
    let _ = read_reply(&mut r);

    let (mut p, mut pr) = conn(port);
    let mut wire = cmd(&["PUBLISH", "a", "one"]);
    wire.extend_from_slice(&cmd(&["PUBLISH", "a", "two"]));
    wire.extend_from_slice(&cmd(&["PUBLISH", "b", "other"])); // channel change: run breaks
    wire.extend_from_slice(&cmd(&["PING"])); // trailing non-publish
    p.write_all(&wire).unwrap();

    assert_eq!(read_reply(&mut pr), b":1\r\n");
    assert_eq!(read_reply(&mut pr), b":1\r\n");
    assert_eq!(read_reply(&mut pr), b":0\r\n"); // channel b has no subscribers
    assert_eq!(read_reply(&mut pr), b"+PONG\r\n");

    assert_eq!(read_delivery(&mut r), "one");
    assert_eq!(read_delivery(&mut r), "two");
}

#[test]
fn no_subscribers_batch_replies_zero() {
    let port = spawn_server();
    let (mut p, mut pr) = conn(port);
    let mut wire = Vec::new();
    for i in 0..50 {
        wire.extend_from_slice(&cmd(&["PUBLISH", "ghost", &format!("g{i}")]));
    }
    p.write_all(&wire).unwrap();
    for _ in 0..50 {
        assert_eq!(read_reply(&mut pr), b":0\r\n");
    }
}

#[test]
fn pattern_subscriber_still_receives_batched_messages() {
    let port = spawn_server();
    let (mut s, mut r) = conn(port);
    s.write_all(&cmd(&["PSUBSCRIBE", "news.*"])).unwrap();
    let _ = read_reply(&mut r); // confirmation

    let (mut p, mut pr) = conn(port);
    let mut wire = Vec::new();
    for i in 0..10 {
        wire.extend_from_slice(&cmd(&["PUBLISH", "news.tech", &format!("t{i}")]));
    }
    p.write_all(&wire).unwrap();
    for _ in 0..10 {
        assert_eq!(read_reply(&mut pr), b":1\r\n");
    }

    // Pattern deliveries arrive as pmessage frames; payload is the last
    // field.
    for i in 0..10 {
        let frame = read_reply(&mut r);
        let text = std::str::from_utf8(&frame).unwrap();
        assert!(text.contains("pmessage"), "not a pmessage: {text:?}");
        assert!(text.contains(&format!("t{i}")), "missing t{i}: {text:?}");
    }
}

// Workers block in poll(); they exit with the test process.
