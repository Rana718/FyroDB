use fyro_db::handler::Conn;
use fyro_db::handler::net::Stream;
use fyro_db::pubsub::PubSub;
use fyro_db::storage::store::Store;
use mio::{Poll, Token, Waker};
use std::sync::Arc;

#[test]
fn test_deadlock_eval_blpop() {
   let store = Arc::new(Store::with_config_workers(1, usize::MAX, 2));
   let pubsub = PubSub::new();
   let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
   let client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
   let (_peer, _) = listener.accept().unwrap();
   client.set_nonblocking(true).unwrap();

   let poll = Poll::new().unwrap();
   let notifier = fyro_db::pubsub::WorkerNotifier::new(
      Arc::new(Waker::new(poll.registry(), Token(usize::MAX)).unwrap()),
      0,
   );

   // Connection 1: BLPOP
   let stream1 = Stream::Tcp(mio::net::TcpStream::from_std(client));
   let mut conn1 = Conn::new(stream1, &store, &pubsub, 1, &notifier, None, 0);

   // Connection 2: EVAL (LPUSH)
   let client2 = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
   let (_peer2, _) = listener.accept().unwrap();
   let stream2 = Stream::Tcp(mio::net::TcpStream::from_std(client2));
   let mut conn2 = Conn::new(stream2, &store, &pubsub, 2, &notifier, None, 0);

   let blocked = b"*3\r\n$5\r\nBLPOP\r\n$1\r\nq\r\n$1\r\n0\r\n";
   conn1.parser.rbuf = blocked.to_vec();
   conn1.parser.filled = blocked.len();
   assert!(conn1.process_buffered() && conn1.is_parked());
   let push = b"*4\r\n$4\r\nEVAL\r\n$38\r\nreturn redis.call('LPUSH',KEYS[1],'v')\r\n$1\r\n1\r\n$1\r\nq\r\n";
   conn2.parser.rbuf = push.to_vec();
   conn2.parser.filled = push.len();
   assert!(conn2.process_buffered());
   let mut completions = Vec::new();
   notifier.drain_completions(&mut completions);
   assert_eq!(completions.len(), 1);
   assert!(conn1.on_completion(completions.pop().unwrap()));
   assert!(!conn1.is_parked());
   assert_eq!(conn1.parser.wbuf, b"*2\r\n$1\r\nq\r\n$1\r\nv\r\n");
}
