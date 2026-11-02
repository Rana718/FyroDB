use std::io::{BufRead, BufReader, Write};
use std::sync::Arc;
use std::time::Duration;

use fyro_db::pubsub::PubSub;
use fyro_db::storage::store::Store;
use fyro_db::worker::run_worker;

#[test]
#[cfg(feature = "unix-sockets")]
fn test_unix_listener_end_to_end() {
   let sock_path = format!("/tmp/fyrodb_test_{}.sock", std::process::id());
   let bind_addr = format!("unix:{}", sock_path);

   let store = Arc::new(Store::with_config(2, 64));
   let pubsub = Arc::new(PubSub::new());

   let store_worker = Arc::clone(&store);
   let pubsub_worker = Arc::clone(&pubsub);
   std::thread::Builder::new()
      .name("unix_listener-worker".to_string())
      .stack_size(512 * 1024)
      .spawn(move || {
         run_worker(&store_worker, &pubsub_worker, 0, &bind_addr, None, 0)
      })
      .unwrap();

   // Wait for listener to be ready
   std::thread::sleep(Duration::from_millis(20));
   if let Ok(stream) = std::os::unix::net::UnixStream::connect(&sock_path) {
      let mut client = stream;
      client
         .set_read_timeout(Some(Duration::from_millis(20)))
         .unwrap();

      let mut reader = BufReader::new(client.try_clone().unwrap());

      // Test PING
      client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
      let mut reply = String::new();
      reader.read_line(&mut reply).unwrap();
      assert_eq!(reply, "+PONG\r\n");

      // Test SET
      client
         .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
         .unwrap();
      let mut set_reply = String::new();
      reader.read_line(&mut set_reply).unwrap();
      assert_eq!(set_reply, "+OK\r\n");

      // Test GET
      client
         .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
         .unwrap();
      let mut line1 = String::new();
      let mut line2 = String::new();
      reader.read_line(&mut line1).unwrap();
      reader.read_line(&mut line2).unwrap();
      assert_eq!(line1, "$3\r\n");
      assert_eq!(line2, "bar\r\n");
   }

   let _ = std::fs::remove_file(&sock_path);
}

#[test]
#[cfg(not(feature = "unix-sockets"))]
#[should_panic(expected = "Unix socket support is disabled")]
fn test_unix_listener_disabled_without_feature() {
   let store = Arc::new(Store::with_config(2, 64));
   let pubsub = Arc::new(PubSub::new());
   run_worker(&store, &pubsub, 0, "unix:/tmp/test.sock", None, 0);
}

#[test]
fn test_tcp_listener_end_to_end() {
   let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
   let port = probe.local_addr().unwrap().port();
   drop(probe);

   let store = Arc::new(Store::with_config(2, 64));
   let pubsub = Arc::new(PubSub::new());

   let store_worker = Arc::clone(&store);
   let pubsub_worker = Arc::clone(&pubsub);
   std::thread::spawn(move || {
      run_worker(&store_worker, &pubsub_worker, port, "127.0.0.1", None, 0);
   });

   // Wait for listener to be ready
   std::thread::sleep(Duration::from_millis(20));
   if let Ok(stream) = std::net::TcpStream::connect(("127.0.0.1", port)) {
      let mut client = stream;
      client.set_nodelay(true).unwrap();
      client
         .set_read_timeout(Some(Duration::from_secs(2)))
         .unwrap();

      let mut reader = BufReader::new(client.try_clone().unwrap());

      // Test PING
      client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
      let mut reply = String::new();
      reader.read_line(&mut reply).unwrap();
      assert_eq!(reply, "+PONG\r\n");

      // Test SET
      client
         .write_all(b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
         .unwrap();
      let mut set_reply = String::new();
      reader.read_line(&mut set_reply).unwrap();
      assert_eq!(set_reply, "+OK\r\n");

      // Test GET
      client
         .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")
         .unwrap();
      let mut line1 = String::new();
      let mut line2 = String::new();
      reader.read_line(&mut line1).unwrap();
      reader.read_line(&mut line2).unwrap();
      assert_eq!(line1, "$5\r\n");
      assert_eq!(line2, "world\r\n");
   }
}
