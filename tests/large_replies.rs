use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Server {
   child: Child,
   port: u16,
   directory: PathBuf,
}

impl Server {
   fn start() -> Self {
      Self::start_with_workers(2)
   }

   fn start_with_workers(workers: usize) -> Self {
      let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
      let port = reservation.local_addr().unwrap().port();
      let directory =
         std::env::temp_dir().join(format!("fyro-large-{}-{port}", std::process::id()));
      std::fs::create_dir_all(&directory).unwrap();
      let mut command = Command::new(env!("CARGO_BIN_EXE_fyro_db"));
      for (name, _) in std::env::vars().filter(|(name, _)| name.starts_with("FYRODB_")) {
         command.env_remove(name);
      }
      command
         .env("FYRODB_BIND", "127.0.0.1")
         .env("FYRODB_PORT", port.to_string())
         .env("FYRODB_WORKERS", workers.to_string())
         .env("FYRODB_SHARDS", "8")
         .env("FYRODB_MAX_KEYS", "0")
         .env("FYRODB_RDB_INTERVAL", "0")
         .env("FYRODB_RDB_PATH", directory.join("snapshot.rdb"))
         .stdout(Stdio::null())
         .stderr(std::fs::File::create(directory.join("server.log")).unwrap());
      drop(reservation);
      let child = command.spawn().unwrap();
      let mut server = Self {
         child,
         port,
         directory,
      };
      let deadline = Instant::now() + Duration::from_secs(5);
      loop {
         if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return server;
         }
         assert!(
            server.child.try_wait().unwrap().is_none(),
            "server exited during startup"
         );
         assert!(Instant::now() < deadline, "server startup timed out");
         std::thread::sleep(Duration::from_millis(10));
      }
   }
}

impl Drop for Server {
   fn drop(&mut self) {
      let _ = self.child.kill();
      let _ = self.child.wait();
      let _ = std::fs::remove_dir_all(&self.directory);
   }
}

#[derive(Debug, PartialEq)]
enum Reply {
   Simple(Vec<u8>),
   Bulk(Vec<u8>),
   Nil,
   Integer(i64),
}

struct Client {
   stream: TcpStream,
   reader: BufReader<TcpStream>,
}

fn append(wire: &mut Vec<u8>, parts: &[&[u8]]) {
   write!(wire, "*{}\r\n", parts.len()).unwrap();
   for part in parts {
      write!(wire, "${}\r\n", part.len()).unwrap();
      wire.extend_from_slice(part);
      wire.extend_from_slice(b"\r\n");
   }
}

impl Client {
   fn connect(port: u16) -> Self {
      let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
      stream.set_nodelay(true).unwrap();
      stream
         .set_read_timeout(Some(Duration::from_secs(10)))
         .unwrap();
      stream
         .set_write_timeout(Some(Duration::from_secs(10)))
         .unwrap();
      let reader = BufReader::new(stream.try_clone().unwrap());
      Self { stream, reader }
   }

   fn read(&mut self) -> Reply {
      let mut line = Vec::new();
      self.reader.read_until(b'\n', &mut line).unwrap();
      assert!(line.ends_with(b"\r\n"), "invalid response header: {line:?}");
      let content = &line[1..line.len() - 2];
      match line[0] {
         b'+' => Reply::Simple(content.to_vec()),
         b':' => Reply::Integer(std::str::from_utf8(content).unwrap().parse().unwrap()),
         b'$' | b'*' if content == b"-1" => Reply::Nil,
         b'$' => {
            let length: usize = std::str::from_utf8(content).unwrap().parse().unwrap();
            let mut payload = vec![0; length];
            self.reader.read_exact(&mut payload).unwrap();
            let mut end = [0; 2];
            self.reader.read_exact(&mut end).unwrap();
            assert_eq!(&end, b"\r\n");
            Reply::Bulk(payload)
         }
         _ => panic!("unexpected response: {line:?}"),
      }
   }

   fn call(&mut self, parts: &[&[u8]]) -> Reply {
      let mut wire = Vec::new();
      append(&mut wire, parts);
      self.stream.write_all(&wire).unwrap();
      self.read()
   }
}

fn exercise_pipeline(mode: u8) {
   exercise_sized_pipeline(mode, 32, 256 * 1024);
}

#[test]
fn one_kibibyte_vectored_batches_keep_unique_alias_and_fallback_replies() {
   for mode in 0..5 {
      exercise_sized_pipeline(mode, 256, 1024);
   }
}

fn exercise_sized_pipeline(mode: u8, count: usize, bytes: usize) {
   exercise_workers_pipeline(mode, count, bytes, 2);
}

#[test]
fn single_core_large_batches_preserve_all_owner_indexes_and_fallbacks() {
   for mode in [0, 2, 3] {
      exercise_workers_pipeline(mode, 256, 64 * 1024, 1);
   }
}

fn exercise_workers_pipeline(mode: u8, count: usize, bytes: usize, workers: usize) {
   let fallback = mode == 1;
   let duplicate = mode == 2;
   let server = Server::start_with_workers(workers);
   let mut consumer = Client::connect(server.port);
   let mut updater = Client::connect(server.port);
   socket2::SockRef::from(&consumer.stream)
      .set_recv_buffer_size(64 * 1024)
      .unwrap();
   let mut values = Vec::new();
   for index in 0..count {
      let length = if mode == 4 && index > 0 {
         [4, 1023, 1025, 65537][index % 4]
      } else {
         bytes
      };
      let mut value = vec![b'a' + (index % 26) as u8; length];
      value[..4].copy_from_slice(&[(index % 128) as u8, 0, b'\r', b'\n']);
      let key = format!("large:{index}");
      assert_eq!(
         updater.call(&[b"SET", key.as_bytes(), &value]),
         Reply::Simple(b"OK".to_vec())
      );
      values.push(value);
   }
   assert_eq!(
      updater.call(&[b"HSET", b"hash", b"field", b"value"]),
      Reply::Integer(1)
   );
   let mut wire = Vec::new();
   for _ in 0..3 {
      append(&mut wire, &[b"PING"]);
   }
   let mut expected = Vec::new();
   for (index, value) in values.iter().enumerate() {
      let key = if (mode == 3 && index == 1) || (duplicate && index == count - 1) {
         "large:0".to_owned()
      } else if (mode == 3 && index == count - 1) || (fallback && index == 1) {
         "missing".to_owned()
      } else if fallback && index == 2 {
         "hash".to_owned()
      } else {
         format!("large:{index}")
      };
      append(&mut wire, &[b"GET", key.as_bytes()]);
      expected.push(
         if (mode == 3 && index == count - 1) || (fallback && (index == 1 || index == 2)) {
            Reply::Nil
         } else if (duplicate && index == count - 1) || (mode == 3 && index == 1) {
            Reply::Bulk(values[0].clone())
         } else {
            Reply::Bulk(value.clone())
         },
      );
   }
   let script_key = if duplicate || mode == 3 {
      "large:0".to_owned()
   } else {
      format!("large:{}", count - 1)
   };
   append(
      &mut wire,
      &[
         b"EVAL",
         b"redis.call('SET',KEYS[1],'from-script'); return 17",
         b"1",
         script_key.as_bytes(),
      ],
   );
   append(&mut wire, &[b"SET", b"reply-ready", b"1"]);
   append(&mut wire, &[b"PING"]);
   consumer.stream.write_all(&wire).unwrap();
   let deadline = Instant::now() + Duration::from_secs(5);
   while updater.call(&[b"GET", b"reply-ready"]) != Reply::Bulk(b"1".to_vec()) {
      assert!(
         Instant::now() < deadline,
         "large response did not release its guards"
      );
      std::thread::sleep(Duration::from_millis(5));
   }
   assert_eq!(
      updater.call(&[b"GET", script_key.as_bytes()]),
      Reply::Bulk(b"from-script".to_vec())
   );
   for index in 0..count {
      let key = format!("large:{index}");
      assert_eq!(
         updater.call(&[b"SET", key.as_bytes(), b"updated"]),
         Reply::Simple(b"OK".to_vec())
      );
   }
   for _ in 0..3 {
      assert_eq!(consumer.read(), Reply::Simple(b"PONG".to_vec()));
   }
   for reply in expected {
      assert_eq!(consumer.read(), reply);
   }
   assert_eq!(consumer.read(), Reply::Integer(17));
   assert_eq!(consumer.read(), Reply::Simple(b"OK".to_vec()));
   assert_eq!(consumer.read(), Reply::Simple(b"PONG".to_vec()));
   assert_eq!(
      consumer.call(&[b"GET", b"large:0"]),
      Reply::Bulk(b"updated".to_vec())
   );
}

#[test]
fn heap_pipeline_handles_changing_sizes_missing_keys_and_other_types() {
   let server = Server::start();
   let mut client = Client::connect(server.port);
   let sizes = [
      64, 23, 24, 25, 63, 65, 128, 256, 257, 512, 1000, 1024, 1025, 4096,
   ];
   for round in 0..3 {
      let values: Vec<_> = (0..64)
         .map(|index| {
            let length = if index == 0 {
               64
            } else {
               sizes[(index + round) % sizes.len()]
            };
            let mut value = vec![b'a' + (index % 26) as u8; length];
            value[..4].copy_from_slice(&[index as u8, 0, b'\r', b'\n']);
            value
         })
         .collect();
      let mut wire = Vec::new();
      for (index, value) in values.iter().enumerate() {
         append(
            &mut wire,
            &[b"SET", format!("key:{index:012}").as_bytes(), value],
         );
      }
      client.stream.write_all(&wire).unwrap();
      for _ in &values {
         assert_eq!(client.read(), Reply::Simple(b"OK".to_vec()));
      }
      assert_eq!(
         client.call(&[b"HSET", b"hash", b"field", b"value"]),
         Reply::Integer(if round == 0 { 1 } else { 0 })
      );
      wire.clear();
      let mut expected = Vec::new();
      for (index, value) in values.iter().enumerate() {
         let key = match index {
            16 => "missing".to_owned(),
            24 => "hash".to_owned(),
            40 => "key:000000000000".to_owned(),
            _ => format!("key:{index:012}"),
         };
         append(&mut wire, &[b"GET", key.as_bytes()]);
         expected.push(match index {
            16 | 24 => Reply::Nil,
            40 => Reply::Bulk(values[0].clone()),
            _ => Reply::Bulk(value.clone()),
         });
      }
      append(&mut wire, &[b"PING"]);
      client.stream.write_all(&wire).unwrap();
      for reply in expected {
         assert_eq!(client.read(), reply);
      }
      assert_eq!(client.read(), Reply::Simple(b"PONG".to_vec()));
   }
}

#[test]
fn large_vectored_replies_survive_backpressure_and_later_overwrites() {
   exercise_pipeline(0);
}

#[test]
fn large_reply_fallbacks_keep_missing_and_wrong_type_positions() {
   exercise_pipeline(1);
}

#[test]
fn duplicate_large_keys_preserve_pending_replies_across_scripts() {
   exercise_pipeline(2);
}

#[test]
fn a_miss_after_duplicate_large_keys_releases_every_read_guard() {
   exercise_pipeline(3);
}

#[test]
fn heap_write_batches_remain_complete_with_readers_and_snapshotting() {
   exercise_heap_snapshot(64);
}

#[test]
fn medium_heap_updates_and_snapshotting_preserve_complete_values() {
   for size in [65, 128, 129, 256, 257, 512, 1024] {
      exercise_heap_snapshot(size);
   }
}

fn exercise_heap_snapshot(size: usize) {
   let server = Server::start();
   let mut setup = Client::connect(server.port);
   for index in 0..64 {
      let key = format!("key:{index:012}");
      assert_eq!(
         setup.call(&[b"SET", key.as_bytes(), &vec![b'0'; size]]),
         Reply::Simple(b"OK".to_vec())
      );
   }
   let barrier = std::sync::Arc::new(std::sync::Barrier::new(7));
   std::thread::scope(|scope| {
      for worker in 0..6 {
         let barrier = std::sync::Arc::clone(&barrier);
         let port = server.port;
         scope.spawn(move || {
            let mut client = Client::connect(port);
            barrier.wait();
            for round in 0..100 {
               let mut wire = Vec::new();
               let value = vec![b'a' + (worker * 2 + round % 2) as u8; size];
               for position in 0..64 {
                  let index = if worker % 2 == 0 {
                     position
                  } else {
                     63 - position
                  };
                  let key = format!("key:{index:012}");
                  if worker < 4 {
                     append(&mut wire, &[b"SET", key.as_bytes(), &value]);
                  } else {
                     append(&mut wire, &[b"GET", key.as_bytes()]);
                  }
               }
               client.stream.write_all(&wire).unwrap();
               for _ in 0..64 {
                  if worker < 4 {
                     assert_eq!(client.read(), Reply::Simple(b"OK".to_vec()));
                  } else {
                     let Reply::Bulk(value) = client.read() else {
                        panic!("missing heap value")
                     };
                     assert_eq!(value.len(), size);
                     assert!(value.iter().all(|b| *b == value[0]));
                  }
               }
            }
         });
      }
      barrier.wait();
      assert_eq!(setup.call(&[b"SAVE"]), Reply::Simple(b"OK".to_vec()));
   });
   let loaded = fyro_db::storage::store::Store::with_config(8, usize::MAX);
   assert_eq!(
      fyro_db::storage::rdb::load(
         &loaded,
         server.directory.join("snapshot.rdb").to_str().unwrap()
      )
      .unwrap(),
      64
   );
   for index in 0..64 {
      let value = loaded.get(&format!("key:{index:012}")).unwrap();
      assert_eq!(value.len(), size);
      assert!(value.as_bytes().iter().all(|b| *b == value.as_bytes()[0]));
   }
}

#[test]
fn watch_aborts_after_a_heap_write_pipeline() {
   let server = Server::start();
   let mut writer = Client::connect(server.port);
   let mut watcher = Client::connect(server.port);
   for index in 0..64 {
      let key = format!("key:{index:012}");
      assert_eq!(
         writer.call(&[b"SET", key.as_bytes(), &[b'a'; 64]]),
         Reply::Simple(b"OK".to_vec())
      );
   }
   assert_eq!(
      watcher.call(&[b"WATCH", b"key:000000000000"]),
      Reply::Simple(b"OK".to_vec())
   );
   assert_eq!(watcher.call(&[b"MULTI"]), Reply::Simple(b"OK".to_vec()));
   assert_eq!(
      watcher.call(&[b"GET", b"key:000000000000"]),
      Reply::Simple(b"QUEUED".to_vec())
   );
   let mut wire = Vec::new();
   for index in 0..64 {
      let key = format!("key:{index:012}");
      append(&mut wire, &[b"SET", key.as_bytes(), &[b'b'; 64]]);
   }
   writer.stream.write_all(&wire).unwrap();
   for _ in 0..64 {
      assert_eq!(writer.read(), Reply::Simple(b"OK".to_vec()));
   }
   assert_eq!(watcher.call(&[b"EXEC"]), Reply::Nil);
}
