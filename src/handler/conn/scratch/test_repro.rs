
#[cfg(test)]
mod tests_repro {
   use super::*;
   use std::sync::Arc;
   use crate::storage::store::Store;
   use crate::pubsub::{PubSub, WorkerNotifier};
   use crate::handler::net::Stream;
   use crate::handler::conn::{Conn, ConnMode};

   #[test]
   fn test_repro_mset_atomic_panic() {
       let store = Arc::new(Store::with_config_workers(2, usize::MAX, 6));
       let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
       let client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
       let (_peer, _) = listener.accept().unwrap();
       client.set_nonblocking(true).unwrap();
       let poll = mio::Poll::new().unwrap();
       let waker = Arc::new(mio::Waker::new(poll.registry(), mio::Token(1)).unwrap());
       let notifier = WorkerNotifier::new(waker, 0);
       let pubsub = PubSub::new();
       let mut conn = Conn::new(
          Stream::Tcp(mio::net::TcpStream::from_std(client)),
          &store,
          &pubsub,
          1,
          &notifier,
          None,
          0,
       );
       
       // MSET key1 value1 key2 value2
       let mset = b"*5\r\n$4\r\nMSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n";
       
       let mut wire = Vec::new();
       wire.extend_from_slice(mset);
       
       conn.parser.filled = wire.len();
       conn.parser.rbuf = wire;
       conn.authenticated = true;
       
       // This should not panic
       let result = conn.process_buffered();
       assert!(result, "process_buffered failed");
   }
}
