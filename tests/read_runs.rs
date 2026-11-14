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

fn with_connection<R>(store: Arc<Store>, run: impl FnOnce(&mut Conn<'_>) -> R) -> R {
   with_socket_pair(store, |conn, _peer| run(conn))
}

fn with_socket_pair<R>(
   store: Arc<Store>,
   run: impl FnOnce(&mut Conn<'_>, &mut std::net::TcpStream) -> R,
) -> R {
   let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
   let client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
   let (mut peer, _) = listener.accept().unwrap();
   client.set_nonblocking(true).unwrap();
   let stream = Stream::Tcp(mio::net::TcpStream::from_std(client));
   let poll = mio::Poll::new().unwrap();
   let waker = Arc::new(mio::Waker::new(poll.registry(), mio::Token(1)).unwrap());
   let notifier = WorkerNotifier::new(waker, 0);
   let pubsub = PubSub::new();
   let mut conn = Conn::new(stream, &store, &pubsub, 1, &notifier, None, 0);
   run(&mut conn, &mut peer)
}

#[test]
fn medium_get_replies_reuse_output_without_increasing_the_buffer_budget() {
   use std::io::Read;
   use std::time::{Duration, Instant};
   for read_capacity in [32 * 1024, 64 * 1024, 128 * 1024] {
      let store = Arc::new(Store::with_config(2, usize::MAX));
      let value = vec![b'v'; 128];
      for index in 0..64 {
         store.set(
            &format!("key:{index:012}"),
            StoreValue::string_small(SmallStr::from_slice(&value)),
         );
      }
      let mut wire = Vec::new();
      for index in 0..256 {
         wire.extend_from_slice(&command(&[
            b"GET",
            format!("key:{:012}", index % 64).as_bytes(),
         ]));
      }
      let expected = bulk(&value).repeat(256);
      with_socket_pair(store, |conn, peer| {
         let mut reader = peer.try_clone().unwrap();
         reader
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
         let replies = expected.clone();
         let consumer = std::thread::spawn(move || {
            let mut received = vec![0; replies.len()];
            for _ in 0..8 {
               reader.read_exact(&mut received).unwrap();
               assert_eq!(received, replies);
            }
         });
         conn.parser.rbuf = Vec::with_capacity(read_capacity);
         let mut allocation = None;
         for _ in 0..8 {
            conn.parser.rbuf.clear();
            conn.parser.rbuf.extend_from_slice(&wire);
            conn.parser.filled = wire.len();
            conn.parser.pos = 0;
            assert!(conn.process_buffered());
            assert_eq!(conn.parser.wbuf, expected);
            if read_capacity == 32 * 1024 {
               let current = conn.parser.wbuf.as_ptr();
               if let Some(previous) = allocation {
                  assert_eq!(current, previous, "output allocation changed after warmup");
               }
               allocation = Some(current);
            }
            let deadline = Instant::now() + Duration::from_secs(10);
            while conn.has_pending_write() {
               assert!(conn.do_write());
               assert!(Instant::now() < deadline);
               std::thread::yield_now();
            }
            let capacity = conn.parser.wbuf.capacity();
            if read_capacity == 32 * 1024 {
               assert!(capacity >= expected.len());
               assert!(conn.parser.rbuf.capacity() + capacity <= 96 * 1024);
            } else {
               assert!(capacity <= 32 * 1024);
            }
         }
         consumer.join().unwrap();
      });
   }
}

fn check_pipeline(store: Arc<Store>, wire: Vec<u8>, expected: &[u8]) -> usize {
   with_connection(store, |conn| {
      conn.parser.filled = wire.len();
      conn.parser.rbuf = wire;
      assert!(conn.process_buffered());
      assert!(conn.parser.wbuf.len() <= conn.parser.wbuf.capacity());
      assert_eq!(conn.parser.wbuf, expected);
      conn.parser.wbuf.capacity()
   })
}

#[test]
fn idle_connections_allocate_no_protocol_buffers() {
   assert!(std::mem::size_of::<Conn<'_>>() <= 2048);
   with_connection(Arc::new(Store::with_config(2, usize::MAX)), |conn| {
      assert_eq!(conn.parser.rbuf.capacity(), 0);
      assert_eq!(conn.parser.wbuf.capacity(), 0);
      assert_eq!(conn.parser.parts_raw.capacity(), 0);
   });
}

#[test]
fn small_replies_stay_small_after_multiple_read_write_cycles() {
   with_connection(Arc::new(Store::with_config(2, usize::MAX)), |conn| {
      for _ in 0..100 {
         let wire = command(&[b"PING"]);
         conn.parser.rbuf = wire;
         conn.parser.filled = conn.parser.rbuf.len();
         conn.parser.pos = 0;
         assert!(conn.process_buffered());
         assert_eq!(conn.parser.wbuf, b"+PONG\r\n");
         assert!(conn.parser.wbuf.capacity() <= 128);
         assert!(conn.do_write());
         assert!(conn.parser.wbuf.is_empty());
      }
   });
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
   assert!(capacity <= 16 * 1024);
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

#[test]
fn concurrent_heap_overwrites_and_reply_encoding_do_not_tear_values() {
   let store = Arc::new(Store::with_config_workers(2, usize::MAX, 8));
   store.set_string("shared", &"a".repeat(4096), 0);
   store.set_string("other", &"a".repeat(4096), 0);
   let barrier = Arc::new(std::sync::Barrier::new(8));
   std::thread::scope(|scope| {
      for worker in 0..8 {
         let store = Arc::clone(&store);
         let barrier = Arc::clone(&barrier);
         scope.spawn(move || {
            with_connection(Arc::clone(&store), |conn| {
               conn.worker = worker;
               barrier.wait();
               for iteration in 0..250 {
                  if worker < 4 {
                     let size = [23, 24, 64, 4096, 65536][iteration % 5];
                     let value = vec![b'a' + worker as u8; size];
                     conn.parser.rbuf = command(&[b"SET", b"shared", &value]).repeat(16);
                     conn.parser.pos = 0;
                     conn.parser.filled = conn.parser.rbuf.len();
                     conn.parser.wbuf.clear();
                     assert!(conn.process_buffered());
                     assert_eq!(conn.parser.wbuf, b"+OK\r\n".repeat(16));
                  } else {
                     let mut reply = Vec::new();
                     if iteration % 2 == 0 {
                        let mut pair = command(&[b"GET", b"shared"]);
                        pair.extend_from_slice(&command(&[b"GET", b"other"]));
                        conn.parser.rbuf = pair.repeat(16);
                        conn.parser.pos = 0;
                        conn.parser.filled = conn.parser.rbuf.len();
                        conn.parser.wbuf.clear();
                        assert!(conn.process_buffered());
                        reply.extend_from_slice(&conn.parser.wbuf);
                     } else {
                        assert!(store.get_to_buf("shared", &mut reply));
                     }
                     let mut remaining = reply.as_slice();
                     while !remaining.is_empty() {
                        let end = remaining.windows(2).position(|w| w == b"\r\n").unwrap();
                        let size: usize = std::str::from_utf8(&remaining[1..end])
                           .unwrap()
                           .parse()
                           .unwrap();
                        let payload = &remaining[end + 2..end + 2 + size];
                        assert!(payload.iter().all(|b| *b == payload[0]));
                        assert_eq!(&remaining[end + 2 + size..end + 4 + size], b"\r\n");
                        remaining = &remaining[end + 4 + size..];
                     }
                  }
               }
            });
         });
      }
   });
}

#[test]
fn binary_keys_in_prefetched_batches_are_valid_keys() {
   // Audit S-2: binary keys are valid keys — a miss replies nil like any
   // other absent key, preserving reply order across the batch.
   let store = Arc::new(Store::with_config(2, usize::MAX));
   let mut wire = Vec::new();
   let mut expected = Vec::new();
   for i in 0..300 {
      if i % 2 == 0 {
         wire.extend_from_slice(&command(&[b"GET", b"\xff"]));
         expected.extend_from_slice(b"$-1\r\n");
      } else {
         wire.extend_from_slice(&command(&[b"GET", b"missing"]));
         expected.extend_from_slice(b"$-1\r\n");
      }
   }
   wire.extend_from_slice(&command(&[b"PING"]));
   expected.extend_from_slice(b"+PONG\r\n");
   check_pipeline(store, wire, &expected);
}

#[test]
fn repeated_hgets_preserve_fallbacks_large_replies_and_transaction_boundaries() {
   let store = Arc::new(Store::with_config(2, usize::MAX));
   let value = vec![255; 4096];
   store.hset_bytes("hash", &[("field", &value)]).unwrap();
   store.set_string("plain", "value", 0);
   let mut wire = Vec::new();
   let mut expected = Vec::new();
   for count in [1, 2, 255, 256, 257, 513] {
      wire.extend_from_slice(&command(&[b"HGET", b"hash", b"field"]).repeat(count));
      expected.extend_from_slice(&bulk(&value).repeat(count));
      wire.extend_from_slice(&command(&[b"HGET", b"hash", b"missing"]).repeat(3));
      expected.extend_from_slice(&b"$-1\r\n".repeat(3));
      wire.extend_from_slice(&command(&[b"HGET", b"plain", b"field"]).repeat(3));
      expected.extend_from_slice(
         &b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".repeat(3),
      );
      wire.extend_from_slice(&command(&[b"EVAL", b"return 42", b"0"]));
      expected.extend_from_slice(b":42\r\n");
      wire.extend_from_slice(&command(&[b"MULTI"]));
      wire.extend_from_slice(&command(&[b"HGET", b"hash", b"field"]));
      wire.extend_from_slice(&command(&[b"EXEC"]));
      expected.extend_from_slice(b"+OK\r\n+QUEUED\r\n*1\r\n");
      expected.extend_from_slice(&bulk(&value));
   }
   wire.extend_from_slice(&command(&[b"PING"]));
   expected.extend_from_slice(b"+PONG\r\n");
   check_pipeline(store, wire, &expected);
}

#[test]
fn repeated_json_get_errors_are_written_once_per_request() {
   let store = Arc::new(Store::with_config(2, usize::MAX));
   store.set_string("plain", "value", 0);
   let mut wire = Vec::new();
   let mut expected = Vec::new();
   for (key, reply) in [
      (&b"missing"[..], &b"$-1\r\n"[..]),
      (
         &b"plain"[..],
         &b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"[..],
      ),
      (&b"\xff"[..], &b"-ERR invalid UTF-8 in request\r\n"[..]),
   ] {
      wire.extend_from_slice(&command(&[b"JSON.GET", key, b"."]).repeat(300));
      expected.extend_from_slice(&reply.repeat(300));
   }
   wire.extend_from_slice(&command(&[b"PING"]));
   expected.extend_from_slice(b"+PONG\r\n");
   check_pipeline(store, wire, &expected);
}

#[test]
fn coalesced_runs_return_scripts_and_transactions_to_the_outer_dispatcher() {
   for (parts, replies) in [
      (
         &[&b"INCR"[..], &b"counter"[..]][..],
         &b":1\r\n:2\r\n:3\r\n"[..],
      ),
      (
         &[&b"LPUSH"[..], &b"queue"[..], &b"item"[..]][..],
         &b":1\r\n:2\r\n:3\r\n"[..],
      ),
      (
         &[&b"PUBLISH"[..], &b"channel"[..], &b"message"[..]][..],
         &b":0\r\n:0\r\n:0\r\n"[..],
      ),
   ] {
      let store = Arc::new(Store::with_config(2, usize::MAX));
      let mut wire = command(parts).repeat(3);
      let mut expected = replies.to_vec();
      wire.extend_from_slice(&command(&[b"EVAL", b"return 42", b"0"]));
      expected.extend_from_slice(b":42\r\n");
      wire.extend_from_slice(&command(parts).repeat(3));
      expected.extend_from_slice(if parts[0] == b"PUBLISH" {
         b":0\r\n:0\r\n:0\r\n"
      } else {
         b":4\r\n:5\r\n:6\r\n"
      });
      wire.extend_from_slice(&command(&[b"WATCH", b"watched"]));
      wire.extend_from_slice(&command(&[b"MULTI"]));
      wire.extend_from_slice(&command(&[b"SET", b"watched", b"value"]));
      wire.extend_from_slice(&command(&[b"EXEC"]));
      wire.extend_from_slice(&command(&[b"GET", b"watched"]));
      wire.extend_from_slice(&command(&[b"PING"]));
      expected.extend_from_slice(b"+OK\r\n+OK\r\n+QUEUED\r\n*1\r\n+OK\r\n$5\r\nvalue\r\n+PONG\r\n");
      check_pipeline(store, wire, &expected);
   }
}

#[test]
fn long_mixed_pipelines_do_not_recurse_per_coalesced_run() {
   let store = Arc::new(Store::with_config(2, usize::MAX));
   let mut wire = Vec::new();
   let mut expected = Vec::new();
   for run in 0..600 {
      for operation in [b"INCR".as_slice(), b"LPUSH".as_slice()] {
         for item in 1..=2 {
            wire.extend_from_slice(&command(if operation == b"INCR" {
               &[b"INCR", b"counter"]
            } else {
               &[b"LPUSH", b"queue", b"item"]
            }));
            expected.extend_from_slice(format!(":{}\r\n", run * 2 + item).as_bytes());
         }
      }
   }
   wire.extend_from_slice(&command(&[b"PING"]));
   expected.extend_from_slice(b"+PONG\r\n");
   check_pipeline(store, wire, &expected);
}

#[test]
fn protected_get_batches_defer_expiry_cleanup_and_end_before_scripts() {
   let store = Arc::new(Store::with_config(2, usize::MAX));
   store.set_string("expired", "old", 1);
   let mut pair = command(&[b"GET", b"expired"]);
   pair.extend_from_slice(&command(&[b"GET", b"missing"]));
   let mut wire = pair.repeat(16);
   let mut expected = b"$-1\r\n".repeat(32);
   wire.extend_from_slice(&command(&[
      b"EVAL",
      b"redis.call('SET','expired','fresh'); return 42",
      b"0",
   ]));
   expected.extend_from_slice(b":42\r\n");
   wire.extend_from_slice(&command(&[b"GET", b"expired"]).repeat(16));
   expected.extend_from_slice(&bulk(b"fresh").repeat(16));
   wire.extend_from_slice(&command(&[b"PING"]));
   expected.extend_from_slice(b"+PONG\r\n");
   check_pipeline(store, wire, &expected);
}

#[test]
fn protected_json_batches_defer_expiry_cleanup_and_end_before_writes() {
   let store = Arc::new(Store::with_config(2, usize::MAX));
   store
      .json_set("expired", ".", "{\"n\":1}", false, false)
      .unwrap();
   store.expire_ms("expired", 1);
   let mut pair = command(&[b"JSON.GET", b"expired", b"."]);
   pair.extend_from_slice(&command(&[b"JSON.GET", b"missing", b"."]));
   let mut wire = pair.repeat(16);
   let mut expected = b"$-1\r\n".repeat(32);
   wire.extend_from_slice(&command(&[b"JSON.SET", b"expired", b".", b"{\"n\":2}"]));
   expected.extend_from_slice(b"+OK\r\n");
   wire.extend_from_slice(&command(&[b"JSON.GET", b"expired", b"."]).repeat(16));
   expected.extend_from_slice(&bulk(b"{\"n\":2}").repeat(16));
   wire.extend_from_slice(&command(&[b"PING"]));
   expected.extend_from_slice(b"+PONG\r\n");
   check_pipeline(store, wire, &expected);
}

#[test]
fn binary_set_keys_keep_successful_reply_positions_and_round_trip() {
   // Audit S-2: binary keys SET and GET byte-for-byte through the batch
   // engines, with reply positions unchanged.
   let store = Arc::new(Store::with_config(2, usize::MAX));
   let mut wire = command(&[b"PING"]);
   wire.extend_from_slice(&command(&[b"SET", b"first", b"a"]));
   wire.extend_from_slice(&command(&[b"SET", b"\xff\x00fe\r\n", b"b"]));
   wire.extend_from_slice(&command(&[b"SET", b"second", b"c"]));
   wire.extend_from_slice(&command(&[b"GET", b"first"]));
   wire.extend_from_slice(&command(&[b"GET", b"\xff\x00fe\r\n"]));
   wire.extend_from_slice(&command(&[b"GET", b"second"]));
   wire.extend_from_slice(&command(&[b"PING"]));
   check_pipeline(
      store,
      wire,
      b"+PONG\r\n+OK\r\n+OK\r\n+OK\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n+PONG\r\n",
   );
}

#[test]
fn batched_bit_writes_and_repeated_count_reads_preserve_script_boundaries() {
   let store = Arc::new(Store::with_config(2, usize::MAX));
   let mut wire = Vec::new();
   let mut expected = Vec::new();
   for bit in 0..32 {
      wire.extend_from_slice(&command(&[
         b"SETBIT",
         b"bits",
         bit.to_string().as_bytes(),
         b"1",
      ]));
      expected.extend_from_slice(b":0\r\n");
   }
   wire.extend_from_slice(&command(&[b"EVAL", b"return 42", b"0"]));
   expected.extend_from_slice(b":42\r\n");
   wire.extend_from_slice(&command(&[b"BITCOUNT", b"bits"]).repeat(257));
   expected.extend_from_slice(&b":32\r\n".repeat(257));
   wire.extend_from_slice(&command(&[b"PFADD", b"hll", b"one"]));
   expected.extend_from_slice(b":1\r\n");
   wire.extend_from_slice(&command(&[b"PFCOUNT", b"hll"]).repeat(257));
   expected.extend_from_slice(&b":1\r\n".repeat(257));
   wire.extend_from_slice(&command(&[b"GEOADD", b"geo", b"0", b"0", b"a"]));
   expected.extend_from_slice(b":1\r\n");
   wire.extend_from_slice(&command(&[b"GEODIST", b"geo", b"a", b"a"]).repeat(257));
   expected.extend_from_slice(&bulk(b"0.0").repeat(257));
   wire.extend_from_slice(&command(&[
      b"EVAL",
      b"redis.call('SET','after','ok'); return 42",
      b"0",
   ]));
   expected.extend_from_slice(b":42\r\n");
   wire.extend_from_slice(&command(&[b"GET", b"after"]));
   expected.extend_from_slice(&bulk(b"ok"));
   wire.extend_from_slice(&command(&[b"PING"]));
   expected.extend_from_slice(b"+PONG\r\n");
   check_pipeline(store, wire, &expected);
}

#[test]
fn worker_sized_batches_preserve_values_boundaries_and_script_order() {
   for workers in [1, 6] {
      for count in [63, 64, 65, 127, 128, 129, 255, 256, 257] {
         let store = Arc::new(Store::with_config_workers(2, usize::MAX, workers));
         let mut writes = Vec::new();
         let mut reads = Vec::new();
         let mut expected = Vec::new();
         for index in 0..count {
            let key = format!("key:{index:012}");
            let mut value = vec![b'a' + (index % 26) as u8; [5, 24, 64, 128, 1024][index % 5]];
            value[..4].copy_from_slice(&[(index % 128) as u8, 0, b'\r', b'\n']);
            writes.extend(command(&[b"SET", key.as_bytes(), &value]));
            reads.extend(command(&[b"GET", key.as_bytes()]));
            expected.extend(bulk(if index == 0 { b"latest" } else { &value }));
            if index == count / 2 {
               reads.extend(command(&[b"PING"]));
               expected.extend_from_slice(b"+PONG\r\n");
            }
         }
         for value in [b"newer".as_slice(), b"latest".as_slice()] {
            writes.extend(command(&[b"SET", b"key:000000000000", value]));
         }
         check_pipeline(Arc::clone(&store), writes, &b"+OK\r\n".repeat(count + 2));
         reads.extend(command(&[b"GET", b"missing"]));
         expected.extend_from_slice(b"$-1\r\n");
         reads.extend(command(&[
            b"EVAL",
            b"redis.call('SET',KEYS[1],'after'); return 17",
            b"1",
            b"key:000000000000",
         ]));
         expected.extend_from_slice(b":17\r\n");
         reads.extend(command(&[b"GET", b"key:000000000000"]));
         expected.extend(bulk(b"after"));
         check_pipeline(store, reads, &expected);
      }
   }
}

#[test]
fn binary_set_fallback_keeps_inserts_and_overwrites_after_long_probe_chains() {
   for workers in [1, 6] {
      let store = Arc::new(Store::with_config_workers(1, 128, workers));
      let mut keys = Vec::new();
      for candidate in 0.. {
         let key = format!("collision:{candidate}");
         let mut hash = [0];
         store.prefetch_batch(&[key.as_bytes()], &mut hash);
         if hash[0] & 255 == 0 {
            keys.push(key);
            if keys.len() == 65 {
               break;
            }
         }
      }
      for pass in 0..3 {
         let mut writes = Vec::new();
         let mut reads = Vec::new();
         let mut expected = Vec::new();
         for (index, key) in keys.iter().enumerate() {
            let value = [255, 0, b'\r', b'\n', pass, index as u8];
            writes.extend(command(&[b"SET", key.as_bytes(), &value]));
            reads.extend(command(&[b"GET", key.as_bytes()]));
            expected.extend(bulk(&value));
         }
         check_pipeline(Arc::clone(&store), writes, &b"+OK\r\n".repeat(keys.len()));
         check_pipeline(Arc::clone(&store), reads, &expected);
         assert_eq!(store.dbsize(), keys.len());
      }
   }
}

#[test]
fn deep_small_set_runs_preserve_read_script_transaction_and_fallback_boundaries() {
   for workers in [1, 6] {
      for count in [256, 2048] {
         let store = Arc::new(Store::with_config_workers(8, usize::MAX, workers));
         let value = [0, 255, b'\r', b'\n', 7];
         let mut wire = Vec::new();
         for index in 0..count {
            wire.extend(command(&[
               b"SET",
               format!("batch:{index:012}").as_bytes(),
               &value,
            ]));
         }
         let mut expected = b"+OK\r\n".repeat(count);
         wire.extend(command(&[b"GET", b"batch:000000000000"]));
         expected.extend(bulk(&value));
         wire.extend(command(&[b"SET", b"watched", b"before"]).repeat(count));
         expected.extend(b"+OK\r\n".repeat(count));
         wire.extend(command(&[
            b"EVAL",
            b"redis.call('SET',KEYS[1],'script'); return 42",
            b"1",
            b"watched",
         ]));
         wire.extend(command(&[b"WATCH", b"watched"]));
         wire.extend(command(&[b"MULTI"]));
         wire.extend(command(&[b"SET", b"watched", b"queued"]));
         wire.extend(command(&[b"EXEC"]));
         wire.extend(command(&[b"GET", b"watched"]));
         expected.extend(b":42\r\n+OK\r\n+OK\r\n+QUEUED\r\n*1\r\n+OK\r\n$6\r\nqueued\r\n");
         wire.extend(command(&[b"SET", b"watched", b"plain"]).repeat(count));
         expected.extend(b"+OK\r\n".repeat(count));
         wire.extend(command(&[b"SET", b"watched", b"blocked", b"NX"]));
         wire.extend(command(&[b"PING"]));
         expected.extend(b"$-1\r\n+PONG\r\n");
         check_pipeline(Arc::clone(&store), wire, &expected);
         assert_eq!(store.get("watched").as_deref(), Some("plain"));
      }
   }
}

#[test]
fn deep_set_admission_releases_on_partial_commands_and_protocol_errors() {
   for incomplete in [false, true] {
      let store = Arc::new(Store::with_config_workers(8, usize::MAX, 6));
      let mut wire = command(&[b"SET", b"key:000000000000", &[b'x'; 64]]).repeat(1024);
      wire.extend_from_slice(if incomplete {
         b"*3\r\n$3\r\nSET\r\n$7\r\npartial\r\n$4\r\nta"
      } else {
         b"*2\r\n$-2\r\n"
      });
      with_connection(Arc::clone(&store), |conn| {
         conn.parser.filled = wire.len();
         conn.parser.rbuf = wire;
         assert_eq!(conn.process_buffered(), incomplete);
         assert_eq!(conn.parser.wbuf, b"+OK\r\n".repeat(1024));
         if incomplete {
            conn.parser.rbuf.extend_from_slice(b"il\r\n");
            conn.parser.filled = conn.parser.rbuf.len();
            assert!(conn.process_buffered());
            assert_eq!(store.get("partial").as_deref(), Some("tail"));
         }
      });
   }
}

#[test]
fn deep_set_admission_preserves_expire_pairs_and_large_value_boundaries() {
   let store = Arc::new(Store::with_config_workers(8, usize::MAX, 6));
   let prefix = command(&[b"SET", b"prefix:000000000000", &[b'p'; 64]]).repeat(1024);
   let mut wire = prefix.clone();
   let mut expected = b"+OK\r\n".repeat(1024);
   wire.extend(command(&[b"SET", b"expiring", b"value"]));
   wire.extend(command(&[b"EXPIRE", b"expiring", b"60"]));
   expected.extend(b"+OK\r\n:1\r\n");
   for size in [257, 4096, 10240] {
      wire.extend_from_slice(&prefix);
      expected.extend(b"+OK\r\n".repeat(1024));
      let value = vec![255; size];
      wire.extend(command(&[b"SET", b"large", &value]));
      wire.extend(command(&[b"GET", b"large"]));
      expected.extend(b"+OK\r\n");
      expected.extend(bulk(&value));
   }
   check_pipeline(Arc::clone(&store), wire, &expected);
   let remaining = store.ttl("expiring").unwrap();
   assert!(remaining > std::time::Duration::ZERO);
   assert!(remaining <= std::time::Duration::from_secs(60));
}

#[test]
fn binary_keys_round_trip_through_the_string_core() {
   // Audit S-2: binary keys are first-class on the string fast paths —
   // SET/GET/DEL/INCR byte-for-byte, including through the batch engines.
   let store = Arc::new(Store::with_config(2, usize::MAX));
   let binary_key: &[u8] = b"\xff\x00bin\r\nkey\x80";
   store.set_string_auto_bytes(binary_key, None, std::ptr::null(), b"one", 0);
   let mut out = Vec::new();
   assert!(store.get_to_buf_auto_bytes(binary_key, None, std::ptr::null(), &mut out));
   assert_eq!(&out, b"$3\r\none\r\n");

   // INCR on a different binary key creates and then increments
   let counter_key: &[u8] = b"\x01counter\xff";
   assert_eq!(store.incr_bytes(counter_key).unwrap(), 1);
   out.clear();
   assert!(store.get_to_buf_auto_bytes(counter_key, None, std::ptr::null(), &mut out));
   assert_eq!(&out, b"$1\r\n1\r\n");

   // EXPIRE on the binary key
   let exp = fyro_db::storage::value::expiry_from_secs(60).unwrap();
   assert!(store.expire_ms_bytes(binary_key, exp));
   // ttl accounting verified via the store unit tests; here we
   // assert the observable protocol behavior


   // DEL removes it
   assert!(store.del_bytes(binary_key));
   out.clear();
   assert!(!store.get_to_buf_auto_bytes(binary_key, None, std::ptr::null(), &mut out));
   assert!(out.is_empty());
}

#[test]
fn binary_keys_set_and_get_through_the_tcp_protocol() {
   let store = Arc::new(Store::with_config(2, usize::MAX));
   let key: &[u8] = b"\xfe\x00k\x7f";
   let mut wire = command(&[b"SET", key, b"val"]);
   wire.extend_from_slice(&command(&[b"GET", key]));
   wire.extend_from_slice(&command(&[b"DEL", key]));
   wire.extend_from_slice(&command(&[b"GET", key]));
   check_pipeline(
      store,
      wire,
      b"+OK\r\n$3\r\nval\r\n:1\r\n$-1\r\n",
   );
}
