use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use fyro_db::pubsub::PubSub;
use fyro_db::storage::store::Store;
use fyro_db::worker::run_worker;

fn spawn_server(auth: Option<&'static str>) -> u16 {
   let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
   let port = probe.local_addr().unwrap().port();
   drop(probe);

   let store = Arc::new(Store::with_config(8, 100_000));
   let pubsub = Arc::new(PubSub::new());

   let s_clone = Arc::clone(&store);
   let p_clone = Arc::clone(&pubsub);
   std::thread::Builder::new()
      .name("auth-worker-0".into())
      .spawn(move || {
         run_worker(&s_clone, &p_clone, port, "127.0.0.1", auth, 0);
      })
      .unwrap();

   std::thread::sleep(Duration::from_millis(100));
   port
}

fn send_cmd(stream: &mut TcpStream, reader: &mut BufReader<TcpStream>, cmd: &str) -> String {
   stream.write_all(cmd.as_bytes()).expect("write cmd");
   stream.flush().expect("flush cmd");
   let mut line = String::new();
   reader.read_line(&mut line).expect("read response");
   line.trim_end().to_string()
}

#[test]
fn test_auth_not_required() {
   let port = spawn_server(None);
   let mut conn = TcpStream::connect(("127.0.0.1", port)).expect("connect");
   conn.set_nodelay(true).unwrap();
   let mut reader = BufReader::new(conn.try_clone().unwrap());

   // Can run commands directly
   let reply = send_cmd(&mut conn, &mut reader, "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n");
   assert_eq!(reply, "+OK");

   // Calling AUTH when not required returns +OK
   let reply = send_cmd(&mut conn, &mut reader, "*2\r\n$4\r\nAUTH\r\n$3\r\nfoo\r\n");
   assert_eq!(reply, "+OK");
}

#[test]
fn test_auth_single_password() {
   let port = spawn_server(Some("supersecret"));
   let mut conn = TcpStream::connect(("127.0.0.1", port)).expect("connect");
   conn.set_nodelay(true).unwrap();
   let mut reader = BufReader::new(conn.try_clone().unwrap());

   // Data command before auth rejected
   let reply = send_cmd(&mut conn, &mut reader, "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n");
   assert!(reply.starts_with("-NOAUTH"), "expected NOAUTH, got: {reply}");

   // Wrong password rejected
   let reply = send_cmd(&mut conn, &mut reader, "*2\r\n$4\r\nAUTH\r\n$5\r\nwrong\r\n");
   assert!(reply.starts_with("-WRONGPASS"), "expected WRONGPASS, got: {reply}");

   // Still unauthenticated
   let reply = send_cmd(&mut conn, &mut reader, "*2\r\n$3\r\nGET\r\n$1\r\na\r\n");
   assert!(reply.starts_with("-NOAUTH"), "expected NOAUTH, got: {reply}");

   // Correct password authenticates
   let reply = send_cmd(&mut conn, &mut reader, "*2\r\n$4\r\nAUTH\r\n$11\r\nsupersecret\r\n");
   assert_eq!(reply, "+OK");

   // Now data commands work
   let reply = send_cmd(&mut conn, &mut reader, "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$3\r\nfoo\r\n");
   assert_eq!(reply, "+OK");
}

#[test]
fn test_auth_username_password_redis6() {
   let port = spawn_server(Some("mypass123"));
   let mut conn = TcpStream::connect(("127.0.0.1", port)).expect("connect");
   conn.set_nodelay(true).unwrap();
   let mut reader = BufReader::new(conn.try_clone().unwrap());

   // AUTH default wrongpass -> rejected
   let reply = send_cmd(
      &mut conn,
      &mut reader,
      "*3\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n$5\r\nwrong\r\n",
   );
   assert!(reply.starts_with("-WRONGPASS"), "expected WRONGPASS, got: {reply}");

   // AUTH default mypass123 -> OK
   let reply = send_cmd(
      &mut conn,
      &mut reader,
      "*3\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n$9\r\nmypass123\r\n",
   );
   assert_eq!(reply, "+OK");

   // Commands succeed
   let reply = send_cmd(&mut conn, &mut reader, "*2\r\n$4\r\nPING\r\n$4\r\ntest\r\n");
   assert_eq!(reply, "$4");
}

#[test]
fn test_auth_hello_inline_and_unauth() {
   let port = spawn_server(Some("hellopass"));
   let mut conn = TcpStream::connect(("127.0.0.1", port)).expect("connect");
   conn.set_nodelay(true).unwrap();
   let mut reader = BufReader::new(conn.try_clone().unwrap());

   // HELLO without auth succeeds, returning array header
   let reply = send_cmd(&mut conn, &mut reader, "*2\r\n$5\r\nHELLO\r\n$1\r\n2\r\n");
   assert!(reply.starts_with('*'), "expected array, got: {reply}");

   // Consume the 14 pairs (28 lines)
   for _ in 0..28 {
      let mut line = String::new();
      reader.read_line(&mut line).unwrap();
   }

   // Still unauthenticated
   let reply = send_cmd(&mut conn, &mut reader, "*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$1\r\ny\r\n");
   assert!(reply.starts_with("-NOAUTH"));

   // HELLO 3 with wrong inline AUTH -> WRONGPASS
   let reply = send_cmd(
      &mut conn,
      &mut reader,
      "*5\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n$5\r\nwrong\r\n",
   );
   assert!(reply.starts_with("-WRONGPASS"));

   // HELLO 3 with correct inline AUTH -> succeeds and authenticates
   let reply = send_cmd(
      &mut conn,
      &mut reader,
      "*5\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n$9\r\nhellopass\r\n",
   );
   assert!(reply.starts_with('%'), "expected RESP3 map, got: {reply}");

   // Consume map entries (7 pairs = 14 lines)
   for _ in 0..14 {
      let mut line = String::new();
      reader.read_line(&mut line).unwrap();
   }

   // Now authenticated
   let reply = send_cmd(&mut conn, &mut reader, "*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$1\r\ny\r\n");
   assert_eq!(reply, "+OK");
}
