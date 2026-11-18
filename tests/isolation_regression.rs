use fyro_db::handler::Conn;
use fyro_db::handler::net::Stream;
use fyro_db::pubsub::{Completion, PubSub, WorkerNotifier};
use fyro_db::storage::store::Store;
use std::io::Read;
use std::sync::Arc;

// Helper function to format commands
fn command(parts: &[&[u8]]) -> Vec<u8> {
   let mut wire = format!("*{}\r\n", parts.len()).into_bytes();
   for part in parts {
      wire.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
      wire.extend_from_slice(part);
      wire.extend_from_slice(b"\r\n");
   }
   wire
}

// Helper to set up a connection and a peer
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
fn blpop_eval_pipelined_gets_reply() {
   let store = Arc::new(Store::with_config(2, usize::MAX));
   with_socket_pair(store, |conn, peer| {
      // Pipeline BLPOP and EVAL
      let mut wire = command(&[b"BLPOP", b"blocking-key", b"0"]);
      wire.extend_from_slice(&command(&[b"EVAL", b"return 123", b"0"]));

      conn.parser.rbuf = wire;
      conn.parser.filled = conn.parser.rbuf.len();
      conn.parser.pos = 0;

      // This should park the connection on BLPOP
      assert!(conn.process_buffered());
      assert!(conn.park.is_some(), "Connection should be parked");
      let conn_uid = conn.uid;
      let conn_token = conn.token;

      // Simulate RPUSH from another connection
      let store_arc = conn.store.clone();
      store_arc.rpush("blocking-key", &["item"]).unwrap();

      // Simulate completion processing
      let completion = Completion::Retry {
         uid: conn_uid,
         token: conn_token,
      };
      assert!(conn.on_completion(completion));
      assert!(conn.park.is_none(), "Connection should be unparked");

      // Check if the reply includes BLPOP and EVAL's result
      assert!(conn.do_write());
      peer
         .set_read_timeout(Some(std::time::Duration::from_secs(3)))
         .unwrap();
      let expected = b"*2\r\n$12\r\nblocking-key\r\n$4\r\nitem\r\n:123\r\n";
      let mut reply = vec![0; expected.len()];
      peer.read_exact(&mut reply).unwrap();
      assert_eq!(reply, expected);
   });
}
