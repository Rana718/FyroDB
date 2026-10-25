use crate::cache::DbCacheManager;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[cfg(unix)]
use std::fs;
#[cfg(unix)]
use std::io::{self, BufRead, BufReader, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
#[cfg(unix)]
use std::path::PathBuf;
#[cfg(unix)]
use std::thread;

const RETRY: Duration = Duration::from_millis(250);
const LOCK_STALE: Duration = Duration::from_secs(5);
const MAX_LINE: usize = 1 << 20;
const FLUSH_LINE: &str = "{\"flush\":true}\n";
const LOCAL_HELLO: &str = "{\"local\":true}\n";

// Publishes table invalidations to sibling FyroDB processes and applies the
// ones they publish. One process automatically becomes the broker; if it dies,
// a survivor takes over. After a reconnect each side flushes locally so an
// invalidation lost while disconnected cannot leave stale rows behind.
pub struct InvalidationBus {
   #[cfg(unix)]
   manager: Arc<DbCacheManager>,
   #[cfg(unix)]
   prefix: String,
   #[cfg(unix)]
   path: PathBuf,
   #[cfg(unix)]
   writer: Mutex<Option<UnixStream>>,
}

impl InvalidationBus {
   pub fn invalidate(&self, tables: &[String]) {
      if tables.is_empty() {
         return;
      }
      let message = serde_json::json!({ "inv": tables });
      self.publish(&format!("{message}\n"));
   }

   pub fn flush(&self) {
      self.publish(FLUSH_LINE);
   }

   #[cfg(unix)]
   fn publish(&self, line: &str) {
      let mut guard = self.writer.lock();
      let Some(stream) = guard.as_mut() else {
         return;
      };
      if stream
         .write_all(line.as_bytes())
         .and_then(|_| stream.flush())
         .is_err()
      {
         let _ = stream.shutdown(std::net::Shutdown::Both);
         *guard = None;
         return;
      }
      self
         .manager
         .metrics()
         .bus_messages_sent
         .fetch_add(1, Ordering::Relaxed);
   }

   #[cfg(not(unix))]
   fn publish(&self, _line: &str) {}

   #[cfg(unix)]
   fn set_connected(&self, connected: bool) {
      self
         .manager
         .metrics()
         .bus_connected
         .store(connected, Ordering::Relaxed);
   }
}

#[cfg(unix)]
pub fn start(
   path: &str,
   manager: Arc<DbCacheManager>,
   prefix: String,
) -> io::Result<Arc<InvalidationBus>> {
   let bus = Arc::new(InvalidationBus {
      manager,
      prefix,
      path: PathBuf::from(path),
      writer: Mutex::new(None),
   });
   let worker = Arc::clone(&bus);
   thread::Builder::new()
      .name("fyrodb-dbcache-bus".into())
      .stack_size(256 * 1024)
      .spawn(move || worker.run())?;
   Ok(bus)
}

#[cfg(not(unix))]
pub fn start(
   path: &str,
   manager: Arc<DbCacheManager>,
   prefix: String,
) -> std::io::Result<Arc<InvalidationBus>> {
   let _ = (path, manager, prefix);
   Ok(Arc::new(InvalidationBus {}))
}

#[cfg(unix)]
impl InvalidationBus {
   fn run(&self) {
      let mut ever_connected = false;
      loop {
         if let Ok(stream) = UnixStream::connect(&self.path) {
            let rejoined = ever_connected;
            if rejoined {
               self.recover();
            }
            // Joining a broker that outlived a previous run means entries in
            // this process may have missed invalidations while it was down.
            if !ever_connected && self.manager.tracked_key_count() > 0 {
               self.manager.flush_all(&self.prefix);
               self.manager.mark_catalog_stale();
            }
            ever_connected = true;
            self.serve_client(stream, rejoined);
         } else if let Some(listener) = self.bind_broker() {
            if ever_connected {
               self.recover();
            }
            ever_connected = true;
            self.serve_broker(listener);
         }
         self.set_connected(false);
         if let Some(stream) = self.writer.lock().take() {
            let _ = stream.shutdown(std::net::Shutdown::Both);
         }
         thread::sleep(RETRY);
      }
   }

   fn recover(&self) {
      self.manager.flush_all(&self.prefix);
      self.manager.mark_catalog_stale();
   }

   fn serve_client(&self, stream: UnixStream, rejoined: bool) {
      let Ok(write_half) = stream.try_clone() else {
         return;
      };
      *self.writer.lock() = Some(write_half);
      self.set_connected(true);
      // Invalidations published while this process was disconnected never left
      // the machine, so make every peer flush; its own cache was cleared by
      // `recover` first.
      if rejoined {
         self.publish(FLUSH_LINE);
      }
      let mut reader = BufReader::new(stream);
      let mut line = String::new();
      loop {
         line.clear();
         match read_line(&mut reader, &mut line) {
            Ok(0) => break,
            Ok(_) => apply_message(&self.manager, &self.prefix, line.trim()),
            Err(_) => break,
         }
      }
   }

   fn bind_broker(&self) -> Option<UnixListener> {
      if UnixStream::connect(&self.path).is_ok() {
         return None;
      }
      // Election lock so two processes that notice the dead broker at the same
      // time cannot both remove and rebind the socket, which would split peers
      // across two brokers. A lock left by a crash is stolen after a grace
      // period since elections finish in milliseconds.
      let lock_path = self
         .path
         .with_file_name(format!("{}.lock", self.path.file_name()?.to_string_lossy()));
      if let Ok(metadata) = fs::metadata(&lock_path)
         && metadata
            .modified()
            .ok()
            .and_then(|modified| modified.elapsed().ok())
            .is_some_and(|age| age > LOCK_STALE)
      {
         let _ = fs::remove_file(&lock_path);
      }
      let Ok(lock) = fs::OpenOptions::new()
         .write(true)
         .create_new(true)
         .open(&lock_path)
      else {
         return None;
      };
      let _ = writeln!(&lock, "{}", std::process::id());
      // A broker may have won between the first check and the lock.
      if UnixStream::connect(&self.path).is_ok() {
         let _ = fs::remove_file(&lock_path);
         return None;
      }
      let _ = fs::remove_file(&self.path);
      let listener = UnixListener::bind(&self.path).ok();
      let _ = fs::remove_file(&lock_path);
      if listener.is_some() {
         let _ = fs::set_permissions(&self.path, fs::Permissions::from_mode(0o600));
      }
      listener
   }

   fn serve_broker(&self, listener: UnixListener) {
      let peers: Arc<Mutex<Vec<Arc<Peer>>>> = Arc::new(Mutex::new(Vec::new()));

      // The broker also needs a publisher path; connect to itself so its own
      // invalidations travel through the same relay as everyone else's.
      if let Ok(mut stream) = UnixStream::connect(&self.path)
         && stream.write_all(LOCAL_HELLO.as_bytes()).is_ok()
         && stream.flush().is_ok()
      {
         *self.writer.lock() = Some(stream);
         self.set_connected(true);
      }

      for incoming in listener.incoming() {
         let Ok(stream) = incoming else {
            break;
         };
         let peer = Arc::new(Peer {
            stream: Mutex::new(stream),
            local: AtomicBool::new(false),
         });
         let reader_stream = match peer.stream.lock().try_clone() {
            Ok(stream) => stream,
            Err(_) => continue,
         };
         peers.lock().push(Arc::clone(&peer));

         let relay = Arc::clone(&peers);
         let manager = Arc::clone(&self.manager);
         let prefix = self.prefix.clone();
         thread::Builder::new()
            .name("fyrodb-dbcache-peer".into())
            .stack_size(128 * 1024)
            .spawn(move || {
               let mut reader = BufReader::new(reader_stream);
               let mut line = String::new();
               let mut first = true;
               loop {
                  line.clear();
                  match read_line(&mut reader, &mut line) {
                     Ok(0) => break,
                     Ok(_) => {}
                     Err(_) => break,
                  }
                  if first {
                     first = false;
                     if serde_json::from_str::<serde_json::Value>(line.trim())
                        .ok()
                        .and_then(|value| value.get("local")?.as_bool())
                        == Some(true)
                     {
                        peer.local.store(true, Ordering::Relaxed);
                        continue;
                     }
                  }
                  // A local publisher already applied this invalidation; relay
                  // only. Remote messages are applied here because the broker
                  // process is often also a proxy.
                  if !peer.local.load(Ordering::Relaxed) {
                     apply_message(&manager, &prefix, line.trim());
                  }
                  let to_relay: Vec<Arc<Peer>> = relay
                     .lock()
                     .iter()
                     .filter(|other| {
                        !Arc::ptr_eq(other, &peer) && !other.local.load(Ordering::Relaxed)
                     })
                     .cloned()
                     .collect();
                  for other in to_relay {
                     let mut stream = other.stream.lock();
                     if stream
                        .write_all(line.as_bytes())
                        .and_then(|_| stream.flush())
                        .is_err()
                     {
                        let _ = stream.shutdown(std::net::Shutdown::Both);
                     }
                  }
               }
               relay.lock().retain(|other| !Arc::ptr_eq(other, &peer));
            })
            .ok();
      }
   }
}

#[cfg(unix)]
struct Peer {
   stream: Mutex<UnixStream>,
   local: AtomicBool,
}

#[cfg(unix)]
fn read_line<R: BufRead>(reader: &mut R, line: &mut String) -> io::Result<usize> {
   let mut count = 0usize;
   loop {
      let available = reader.fill_buf()?;
      if available.is_empty() {
         break;
      }
      let take = available
         .iter()
         .position(|byte| *byte == b'\n')
         .map_or(available.len(), |index| index + 1);
      count += take;
      if count > MAX_LINE {
         return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalidation message too large",
         ));
      }
      line.push_str(&String::from_utf8_lossy(&available[..take]));
      let done = available[take - 1] == b'\n';
      reader.consume(take);
      if done {
         break;
      }
   }
   Ok(count)
}

#[cfg(unix)]
fn apply_message(manager: &Arc<DbCacheManager>, prefix: &str, line: &str) {
   let Ok(message) = serde_json::from_str::<serde_json::Value>(line) else {
      return;
   };
   let metrics = manager.metrics();
   if message.get("flush").and_then(|value| value.as_bool()) == Some(true) {
      metrics
         .bus_messages_received
         .fetch_add(1, Ordering::Relaxed);
      manager.flush_all(prefix);
      manager.mark_catalog_stale();
      return;
   }
   let Some(tables) = message.get("inv").and_then(|value| value.as_array()) else {
      return;
   };
   let tables: Vec<String> = tables
      .iter()
      .filter_map(|value| value.as_str().map(str::to_string))
      .collect();
   if tables.is_empty() {
      return;
   }
   metrics
      .bus_messages_received
      .fetch_add(1, Ordering::Relaxed);
   manager.invalidate_tables(&tables);
}
