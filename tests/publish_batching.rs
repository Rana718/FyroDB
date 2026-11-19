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
         .spawn(move || fyro_db::worker::run_worker(&store, &pubsub, port, "127.0.0.1", None, idx))
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
      let n: usize = line.get(1..).unwrap().trim().parse().unwrap();
      let mut payload = vec![0u8; n + 2];
      r.read_exact(&mut payload).unwrap();
      out.extend_from_slice(&payload);
   } else if line.starts_with('*') {
      let n: usize = line.get(1..).unwrap().trim().parse().unwrap();
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

fn binary_command(parts: &[&[u8]]) -> Vec<u8> {
   let mut out = format!("*{}\r\n", parts.len()).into_bytes();
   for part in parts {
      out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
      out.extend_from_slice(part);
      out.extend_from_slice(b"\r\n");
   }
   out
}

#[test]
fn long_channel_direct_and_pattern_deliveries_keep_binary_payloads_and_order() {
   let port = spawn_server();
   let channel = "n".repeat(65);
   let pattern = format!("{}*", "n".repeat(60));
   let (mut sub, mut reader) = conn(port);
   sub.write_all(&cmd(&["SUBSCRIBE", &channel])).unwrap();
   read_reply(&mut reader);
   sub.write_all(&cmd(&["PSUBSCRIBE", &pattern])).unwrap();
   read_reply(&mut reader);
   let (mut publisher, mut replies) = conn(port);
   let sizes = [5, 23, 24, 64, 256, 1024, 4096, 10240];
   let mut expected = Vec::new();
   let mut wire = Vec::new();
   for i in 0..64 {
      let mut value = vec![i as u8; sizes[i % sizes.len()]];
      value[..4].copy_from_slice(&[255, 0, 13, 10]);
      wire.extend_from_slice(&binary_command(&[b"PUBLISH", channel.as_bytes(), &value]));
      let mut direct = binary_command(&[b"message", channel.as_bytes(), &value]);
      direct.extend_from_slice(&binary_command(&[
         b"pmessage",
         pattern.as_bytes(),
         channel.as_bytes(),
         &value,
      ]));
      expected.push(direct);
   }
   publisher.write_all(&wire).unwrap();
   for _ in 0..64 {
      assert_eq!(read_reply(&mut replies), b":2\r\n");
   }
   for expected in expected {
      let mut received = read_reply(&mut reader);
      received.extend_from_slice(&read_reply(&mut reader));
      assert_eq!(received, expected);
   }
   publisher.write_all(&cmd(&["PING"])).unwrap();
   assert_eq!(read_reply(&mut replies), b"+PONG\r\n");
}

#[test]
fn a_slow_subscriber_is_disconnected_without_unbounded_backlog_or_server_failure() {
   let port = spawn_server();
   let (mut sub, mut reader) = conn(port);
   let small_receive = 4096i32;
   use std::os::fd::AsRawFd;
   assert_eq!(
      unsafe {
         libc::setsockopt(
            sub.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            (&small_receive as *const i32).cast(),
            std::mem::size_of::<i32>() as _,
         )
      },
      0
   );
   sub.write_all(&cmd(&["SUBSCRIBE", "slow"])).unwrap();
   read_reply(&mut reader);
   let (mut publisher, mut replies) = conn(port);
   let value = vec![b'x'; 10240];
   let batch = binary_command(&[b"PUBLISH", b"slow", &value]).repeat(128);
   let mut disconnected = false;
   for _ in 0..80 {
      publisher.write_all(&batch).unwrap();
      for _ in 0..128 {
         let count = read_reply(&mut replies);
         assert!(count == b":1\r\n" || count == b":0\r\n");
         disconnected |= count == b":0\r\n";
      }
      if disconnected {
         break;
      }
   }
   assert!(
      disconnected,
      "subscriber remained connected after exceeding its byte budget"
   );
   publisher.write_all(&cmd(&["PING"])).unwrap();
   assert_eq!(read_reply(&mut replies), b"+PONG\r\n");
   publisher
      .write_all(&cmd(&["SET", "after-overload", "safe"]))
      .unwrap();
   assert_eq!(read_reply(&mut replies), b"+OK\r\n");
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

#[test]
fn batched_subscription_generates_incrementing_answers() {
   let port = spawn_server();
   let (mut s, mut r) = conn(port);
   s.write_all(&cmd(&["SUBSCRIBE", "a", "b"])).unwrap();
   let ans = read_reply(&mut r);
   assert_eq!(
      String::from_utf8(ans).unwrap(),
      "*3\r\n$9\r\nsubscribe\r\n$1\r\na\r\n:1\r\n"
   );
   let ans = read_reply(&mut r);
   assert_eq!(
      String::from_utf8(ans).unwrap(),
      "*3\r\n$9\r\nsubscribe\r\n$1\r\nb\r\n:2\r\n"
   );
}

#[test]
fn batched_unsubscription_generates_decrementing_answers() {
   let port = spawn_server();
   let (mut s, mut r) = conn(port);
   s.write_all(&cmd(&["SUBSCRIBE", "a", "b"])).unwrap();
   let _ = read_reply(&mut r);
   s.write_all(&cmd(&["UNSUBSCRIBE", "a", "b"])).unwrap();
   let _ = read_reply(&mut r);
   let ans = read_reply(&mut r);
   assert_eq!(
      String::from_utf8(ans).unwrap(),
      "*3\r\n$11\r\nunsubscribe\r\n$1\r\na\r\n:1\r\n"
   );
   let ans = read_reply(&mut r);
   assert_eq!(
      String::from_utf8(ans).unwrap(),
      "*3\r\n$11\r\nunsubscribe\r\n$1\r\nb\r\n:0\r\n"
   );
}

// Workers block in poll(); they exit with the test process.
