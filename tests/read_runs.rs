//! Exercise complete read pipelines in one parser buffer, so batching does
//! not depend on how TCP splits the requests into packets.

use std::sync::Arc;

use fyro_db::handler::Conn;
use fyro_db::handler::net::Stream;
use fyro_db::pubsub::{PubSub, WorkerNotifier};
use fyro_db::storage::{
   store::Store,
   value::{SmallStr, StoreValue},
};

fn command(parts: &[&[u8]]) -> Vec<u8> {
   let mut wire = format!("*{}\r\n", parts.len()).into_bytes();
   for part in parts {
      wire.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
      wire.extend_from_slice(part);
      wire.extend_from_slice(b"\r\n");
   }
   wire
}

fn bulk(value: &[u8]) -> Vec<u8> {
   let mut reply = format!("${}\r\n", value.len()).into_bytes();
   reply.extend_from_slice(value);
   reply.extend_from_slice(b"\r\n");
   reply
}

fn check_pipeline(store: Arc<Store>, wire: Vec<u8>, expected: &[u8]) -> usize {
   let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
   let client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
   let (_peer, _) = listener.accept().unwrap();
   client.set_nonblocking(true).unwrap();
   let stream = Stream::Tcp(mio::net::TcpStream::from_std(client));
   let poll = mio::Poll::new().unwrap();
   let waker = Arc::new(mio::Waker::new(poll.registry(), mio::Token(1)).unwrap());
   let notifier = WorkerNotifier::new(waker, 0);
   let pubsub = PubSub::new();
   let mut conn = Conn::new(stream, &store, &pubsub, 1, &notifier, None, 0);
   conn.parser.filled = wire.len();
   conn.parser.rbuf = wire;
   assert!(conn.process_buffered());
   assert!(conn.parser.wbuf.len() <= conn.parser.wbuf.capacity());
   assert_eq!(conn.parser.wbuf, expected);
   conn.parser.wbuf.capacity()
}

#[test]
fn repeated_gets_reserve_the_encoded_size_without_growing_the_idle_buffer() {
   let store = Arc::new(Store::with_config(2, usize::MAX));
   let mut wire = Vec::new();
   for _ in 0..250 {
      wire.extend_from_slice(&command(&[b"SET", b"hot", b"value"]));
   }
   for _ in 0..250 {
      wire.extend_from_slice(&command(&[b"GET", b"hot"]));
   }
   let mut expected = b"+OK\r\n".repeat(250);
   expected.extend_from_slice(&bulk(b"value").repeat(250));
   let capacity = check_pipeline(store, wire, &expected);
   assert_eq!(capacity, 16 * 1024);
}

#[test]
fn pipelined_get_large_and_repeated_values() {
   let store = Arc::new(Store::with_config(2, usize::MAX));
   let mut wire = Vec::new();
   let mut expected = Vec::new();
   // Cover inline/heap and RESP length boundaries, including large values.
   for (index, size) in [0, 1, 9, 10, 23, 24, 56, 57, 64, 99, 100, 4096, 65536]
      .into_iter()
      .enumerate()
   {
      let key = format!("key:{index}");
      let value = "x".repeat(size);
      store.set(&key, StoreValue::string_str(&value));
      for _ in 0..24 {
         wire.extend_from_slice(&command(&[b"GET", key.as_bytes()]));
         expected.extend_from_slice(&bulk(value.as_bytes()));
      }
      // Force a fallback between hits, including after a large reply.
      wire.extend_from_slice(&command(&[b"GET", b"missing"]));
      expected.extend_from_slice(b"$-1\r\n");
   }
   wire.extend_from_slice(&command(&[b"PING"]));
   expected.extend_from_slice(b"+PONG\r\n");
   check_pipeline(store, wire, &expected);
}

#[test]
fn pipelined_get_repeat_boundaries_preserve_prefix_and_order() {
   let store = Arc::new(Store::with_config(2, usize::MAX));
   let value = "x".repeat(64);
   store.set("repeated", StoreValue::string_str(&value));
   let mut wire = Vec::new();
   let mut expected = Vec::new();
   for count in [1, 2, 3, 127, 255, 256, 257, 513] {
      wire.extend_from_slice(&command(&[b"PING"]));
      expected.extend_from_slice(b"+PONG\r\n");
      for _ in 0..count {
         wire.extend_from_slice(&command(&[b"GET", b"repeated"]));
         expected.extend_from_slice(&bulk(value.as_bytes()));
      }
   }
   check_pipeline(store, wire, &expected);
}

#[test]
fn pipelined_get_distinct_large_values_across_batches() {
   let store = Arc::new(Store::with_config(2, usize::MAX));
   let mut wire = Vec::new();
   let mut expected = Vec::new();
   // Cover distinct binary values across the 256-command batch boundary.
   for index in 0..300 {
      let key = format!("distinct:{index}");
      let mut value = vec![(index % 256) as u8; 4096];
      value[..4].copy_from_slice(&(index as u32).to_le_bytes());
      store.set(&key, StoreValue::string_small(SmallStr::from_slice(&value)));
      wire.extend_from_slice(&command(&[b"GET", key.as_bytes()]));
      expected.extend_from_slice(&bulk(&value));
   }
   wire.extend_from_slice(&command(&[b"PING"]));
   expected.extend_from_slice(b"+PONG\r\n");
   check_pipeline(store, wire, &expected);
}

#[test]
fn pipelined_json_get_large_values_and_fallbacks() {
   let store = Arc::new(Store::with_config(2, usize::MAX));
   let mut wire = Vec::new();
   let mut expected = Vec::new();
   store.set("plain", StoreValue::string_str("not JSON"));
   for (index, size) in [1, 64, 4096, 65536].into_iter().enumerate() {
      let key = format!("json:{index}");
      let value = format!("{{\"payload\":\"{}\"}}", "x".repeat(size));
      assert_eq!(store.json_set(&key, ".", &value, false, false), Ok(true));
      for _ in 0..80 {
         wire.extend_from_slice(&command(&[b"JSON.GET", key.as_bytes(), b"."]));
         expected.extend_from_slice(&bulk(value.as_bytes()));
      }
      wire.extend_from_slice(&command(&[b"JSON.GET", b"missing", b"."]));
      expected.extend_from_slice(b"$-1\r\n");
      wire.extend_from_slice(&command(&[b"JSON.GET", b"plain", b"."]));
      expected.extend_from_slice(
         b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
      );
   }
   wire.extend_from_slice(&command(&[b"PING"]));
   expected.extend_from_slice(b"+PONG\r\n");
   check_pipeline(store, wire, &expected);
}
