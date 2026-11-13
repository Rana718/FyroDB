use mio::net::TcpListener;
#[cfg(feature = "unix-sockets")]
use mio::net::UnixListener;
use mio::{Events, Interest, Poll, Token, Waker};
use socket2::{Domain, Protocol, Socket, Type};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::handler::Conn;
use crate::handler::conn::ConnMode;
use crate::handler::net::{Listener, Stream};
use crate::pubsub::{PubSub, WorkerNotifier};
use crate::storage::store::Store;
use crate::storage::value::tick_clock;

const LISTENER_TOKEN: Token = Token(0);
const WAKER_TOKEN: Token = Token(usize::MAX);

static MAX_CLIENTS: AtomicUsize = AtomicUsize::new(10_000);
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

const SLOW_SUB_MSG_CAP: usize = 262_144;

pub fn set_max_clients(n: usize) {
   MAX_CLIENTS.store(n, Ordering::Relaxed);
}

pub fn initiate_shutdown() {
   SHUTDOWN.store(true, Ordering::Release);
}

pub fn run_worker(
   store: &Arc<Store>,
   pubsub: &PubSub,
   port: u16,
   bind: &str,
   auth: Option<&str>,
   worker_index: usize,
) {
   let mut listener = make_listener(port, bind, worker_index);

   let mut poll = Poll::new().unwrap();
   let mut events = Events::with_capacity(512);

   poll
      .registry()
      .register(&mut listener, LISTENER_TOKEN, Interest::READABLE)
      .unwrap();

   let waker = Arc::new(Waker::new(poll.registry(), WAKER_TOKEN).unwrap());
   let notifier = WorkerNotifier::new(waker, worker_index);

   // A Conn is large; a fixed 4096-slot reservation is a sizeable idle
   // allocation. Grow with actual connections.
   let mut conns: Vec<Option<Conn>> = Vec::new();
   let mut next_token: usize = 1;
   let mut free: Vec<usize> = Vec::new();
   let mut dirty: Vec<usize> = Vec::with_capacity(32);
   let mut sub_dirty: Vec<usize> = Vec::with_capacity(16);
   let mut fanout_scratch: Vec<crate::pubsub::FanEntry> = Vec::new();
   // Generation-stamped marks: one u32 store per touched connection instead
   // of filling the whole table each fanout cycle.
   let mut fanout_seen: Vec<u32> = Vec::new();
   let mut fanout_gen: u32 = 0;
   let mut completions: Vec<crate::pubsub::Completion> = Vec::new();
   let mut expired: Vec<(usize, u64)> = Vec::new();
   let busy_window = std::env::var("FYRODB_BUSY_POLL_US")
      .ok()
      .and_then(|v| v.parse::<u64>().ok())
      .filter(|&us| us > 0)
      .map(std::time::Duration::from_micros);
   let mut last_active = std::time::Instant::now();
   #[cfg(feature = "io-uring")]
   let mut send_ring = crate::handler::uring::SendRing::from_env();
   #[cfg(feature = "io-uring")]
   let mut sent: Vec<(u64, i32)> = Vec::new();

   loop {
      if SHUTDOWN.load(Ordering::Acquire) {
         for conn in conns.iter_mut().flatten() {
            let _ = conn.do_write();
         }
         #[cfg(feature = "unix-sockets")]
         #[allow(clippy::collapsible_if)]
         if worker_index == 0 {
            if let Some(path) = parse_unix_path(bind) {
               let _ = std::fs::remove_file(path);
            }
         }
         return;
      }

      let timeout = if let Some(window) = busy_window
         && last_active.elapsed() < window
      {
         Some(std::time::Duration::ZERO)
      } else {
         let mut timeout = (!sub_dirty.is_empty()).then_some(std::time::Duration::from_micros(50));
         // Wake for the earliest blocked-command deadline.
         if let Some(deadline) = crate::handler::conn::park::next_deadline() {
            let until = deadline.saturating_duration_since(std::time::Instant::now());
            timeout = Some(timeout.map_or(until, |t| t.min(until)));
         }
         timeout
      };

      match poll.poll(&mut events, timeout) {
         Ok(_) => {}
         Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
         Err(_) => continue,
      }

      tick_clock();
      if busy_window.is_some() && !events.is_empty() {
         last_active = std::time::Instant::now();
      }

      for event in events.iter() {
         match event.token() {
            LISTENER_TOKEN => loop {
               match listener.accept() {
                  Ok(mut stream) => {
                     let current = store.connected_clients();
                     let max = MAX_CLIENTS.load(Ordering::Relaxed);
                     if current >= max {
                        let _ = std::io::Write::write_all(
                           &mut stream,
                           b"-ERR max number of clients reached\r\n",
                        );
                        drop(stream);
                        continue;
                     }

                     match stream {
                        Stream::Tcp(ref tcp) => {
                           let sock_ref = socket2::SockRef::from(tcp);
                           let _ = sock_ref.set_tcp_nodelay(true);
                           #[cfg(target_os = "linux")]
                           let _ = sock_ref.set_tcp_quickack(true);
                        }
                        #[cfg(feature = "unix-sockets")]
                        Stream::Unix(_) => {}
                     }
                     let id = alloc_slot(&mut free, &mut next_token, &mut conns);
                     poll
                        .registry()
                        .register(&mut stream, Token(id), Interest::READABLE)
                        .unwrap();
                     conns[id] = Some(Conn::new(
                        stream,
                        store,
                        pubsub,
                        id,
                        &notifier,
                        auth,
                        worker_index,
                     ));
                  }
                  Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                  Err(_) => break,
               }
            },

            WAKER_TOKEN => {
               if notifier.take_fanout_overflow() {
                  for id in 0..conns.len() {
                     if conns[id]
                        .as_ref()
                        .is_some_and(|conn| matches!(conn.mode, ConnMode::Subscribed { .. }))
                     {
                        close_conn(&mut conns, &mut poll, &mut free, id);
                     }
                  }
               }
               notifier.drain_completions(&mut completions);
               for c in completions.drain(..) {
                  let token = match &c {
                     crate::pubsub::Completion::Reply { token, .. }
                     | crate::pubsub::Completion::Retry { token, .. } => *token,
                  };
                  let keep = match conns.get_mut(token).and_then(|s| s.as_mut()) {
                     Some(conn) => conn.on_completion(c),
                     None => continue,
                  };
                  if keep {
                     dirty.push(token);
                  } else {
                     close_conn(&mut conns, &mut poll, &mut free, token);
                  }
               }
               fanout_gen = fanout_gen.wrapping_add(1);
               if fanout_gen == 0 {
                  fanout_seen.fill(0);
                  fanout_gen = 1;
               }
               fanout_seen.resize(conns.len(), 0);
               for &id in &sub_dirty {
                  if id < fanout_seen.len() {
                     fanout_seen[id] = fanout_gen;
                  }
               }
               let pre_drain = sub_dirty.len();
               notifier.drain_pending_into(&mut sub_dirty);
               {
                  let mut w = pre_drain;
                  for r in pre_drain..sub_dirty.len() {
                     let id = sub_dirty[r];
                     if id < fanout_seen.len() && fanout_seen[id] != fanout_gen {
                        fanout_seen[id] = fanout_gen;
                        sub_dirty[w] = id;
                        w += 1;
                     }
                  }
                  sub_dirty.truncate(w);
               }
               deliver_fanout(
                  &notifier,
                  &mut conns,
                  &mut sub_dirty,
                  &mut fanout_scratch,
                  &mut fanout_seen,
                  fanout_gen,
               );
            }

            token => {
               let id = token.0;
               if let Some(conn) = conns.get_mut(id).and_then(|s| s.as_mut()) {
                  let close = (event.is_readable() && !conn.do_read())
                     || (event.is_writable() && !conn.do_write());
                  if !close {
                     let wants_writable = conn.has_pending_write();
                     if wants_writable {
                        dirty.push(id);
                     } else if conn.writable_registered {
                        sync_write_interest(&mut conns, &mut poll, id);
                     }
                  } else {
                     close_conn(&mut conns, &mut poll, &mut free, id);
                  }
               }
            }
         }
      }

      crate::handler::conn::park::take_expired_into(&mut expired);
      if !expired.is_empty() {
         let now = std::time::Instant::now();
         for (token, uid) in expired.drain(..) {
            let keep = match conns.get_mut(token).and_then(|s| s.as_mut()) {
               Some(conn) => conn.on_timeout(uid, now),
               None => continue,
            };
            if keep {
               dirty.push(token);
            } else {
               close_conn(&mut conns, &mut poll, &mut free, token);
            }
         }
      }

      crate::storage::persistence::aof::flush_local();
      if dirty.len() == 2 {
         if dirty[0] == dirty[1] {
            dirty.pop();
         } else if dirty[0] > dirty[1] {
            dirty.swap(0, 1);
         }
      } else if dirty.len() > 2 {
         dirty.sort_unstable();
         dirty.dedup();
      }
      #[cfg(feature = "io-uring")]
      if let Some(ring) = send_ring.as_mut() {
         sent.clear();
         sub_dirty.sort_unstable();
         sub_dirty.dedup();
         sub_dirty.retain(|&id| {
            if conns
               .get(id)
               .and_then(|c| c.as_ref())
               .is_some_and(is_slow_subscriber)
            {
               close_conn(&mut conns, &mut poll, &mut free, id);
               false
            } else {
               true
            }
         });
         for id in dirty.iter().copied().chain(
            sub_dirty
               .iter()
               .copied()
               .filter(|id| dirty.binary_search(id).is_err()),
         ) {
            if let Some(Some(conn)) = conns.get_mut(id)
               && let Some((fd, ptr, len)) = conn.prepare_send()
            {
               // Each connection is submitted once; buffers stay untouched until all CQEs arrive.
               unsafe { ring.push(fd, ptr, len, id as u64, &mut sent) };
            }
         }
         ring.complete(&mut sent);
         for &(id, res) in &sent {
            let id = id as usize;
            let keep = conns
               .get_mut(id)
               .and_then(|s| s.as_mut())
               .is_some_and(|conn| conn.finish_send(res));
            if !keep {
               close_conn(&mut conns, &mut poll, &mut free, id);
            } else {
               sync_write_interest(&mut conns, &mut poll, id);
            }
         }
         dirty.retain(|&id| {
            conns
               .get(id)
               .and_then(|s| s.as_ref())
               .is_some_and(|c| c.has_pending_write())
         });
         sub_dirty.retain(|&id| {
            conns
               .get(id)
               .and_then(|s| s.as_ref())
               .is_some_and(|c| c.has_pending_write())
         });
      }
      for id in dirty.drain(..) {
         let close = match conns.get_mut(id).and_then(|s| s.as_mut()) {
            Some(conn) => !conn.do_write(),
            None => continue,
         };
         if close {
            close_conn(&mut conns, &mut poll, &mut free, id);
         } else {
            sync_write_interest(&mut conns, &mut poll, id);
         }
      }

      flush_fanout(
         &notifier,
         &mut conns,
         &mut sub_dirty,
         &mut fanout_scratch,
         &mut fanout_seen,
         &mut fanout_gen,
      );

      let mut i = 0;
      while i < sub_dirty.len() {
         let id = sub_dirty[i];
         if let Some(Some(conn)) = conns.get_mut(id) {
            if is_slow_subscriber(conn) {
               close_conn(&mut conns, &mut poll, &mut free, id);
               sub_dirty.swap_remove(i);
               continue;
            }
            if !conn.do_write() {
               close_conn(&mut conns, &mut poll, &mut free, id);
               sub_dirty.swap_remove(i);
               continue;
            }
            if !conn.has_pending_write() {
               sync_write_interest(&mut conns, &mut poll, id);
               sub_dirty.swap_remove(i);
               continue;
            }
            sync_write_interest(&mut conns, &mut poll, id);
            i += 1;
         } else {
            sub_dirty.swap_remove(i);
         }
      }

      if free.len() > conns.len() / 2 && conns.len() > 1024 {
         while conns.last().is_some_and(|s| s.is_none()) {
            conns.pop();
         }
         free.retain(|&id| id < conns.len());
         next_token = conns.len();
         conns.shrink_to_fit();
      }
   }
}

#[inline]
fn is_slow_subscriber(conn: &Conn) -> bool {
   if let ConnMode::Subscribed { ref slot, .. } = conn.mode {
      let (bytes, overflow) = slot.backlog_state();
      overflow
         || slot.limit_exceeded(conn.parser.wbuf.len().saturating_sub(conn.write_offset))
         || (bytes != 0 && slot.queue_len() > SLOW_SUB_MSG_CAP)
   } else {
      false
   }
}

#[inline]
fn flush_fanout(
   notifier: &Arc<crate::pubsub::WorkerNotifier>,
   conns: &mut [Option<Conn>],
   sub_dirty: &mut Vec<usize>,
   scratch: &mut Vec<crate::pubsub::FanEntry>,
   seen: &mut Vec<u32>,
   fgen: &mut u32,
) {
   if notifier.fanout.is_empty() {
      return;
   }
   *fgen = fgen.wrapping_add(1);
   if *fgen == 0 {
      seen.fill(0);
      *fgen = 1;
   }
   if conns.len() > seen.len() {
      seen.resize(conns.len(), 0);
   }
   for &id in sub_dirty.iter() {
      if id < seen.len() {
         seen[id] = *fgen;
      }
   }
   deliver_fanout(notifier, conns, sub_dirty, scratch, seen, *fgen);
}

/// Frames append straight to wbuf (memcpy); a connection past the batch
/// cap spills into its slot queue (backlog drain, shedding intact).
fn deliver_fanout(
   notifier: &WorkerNotifier,
   conns: &mut [Option<Conn>],
   sub_dirty: &mut Vec<usize>,
   scratch: &mut Vec<crate::pubsub::FanEntry>,
   seen: &mut [u32],
   fgen: u32,
) {
   scratch.clear();
   notifier.drain_fanout_limit(1024, 1024 * 1024, |entry| scratch.push(entry));
   if scratch.is_empty() {
      return;
   }
   notifier.with_local_subs(|local| {
      let Some(local) = local else { return };
      let first = &scratch[0];
      let grouped = scratch.len() > 1 && scratch.iter().all(|entry| entry.same_subscription(first));
      if grouped {
         let subscriptions = if first.is_pattern() {
            &local.patterns
         } else {
            &local.channels
         };
         let Some(members) = subscriptions.get(first.channel.as_ref()) else {
            return;
         };
         let total = scratch
            .iter()
            .fold(0usize, |n, entry| n.saturating_add(entry.frame.len()));
         for member in members {
            if member.generation > first.generation() {
               continue;
            }
            let token = member.token;
            let Some(Some(conn)) = conns.get_mut(token) else {
               continue;
            };
            let ConnMode::Subscribed {
               slot,
               channels,
               patterns,
            } = &conn.mode
            else {
               continue;
            };
            if !(if first.is_pattern() {
               patterns.contains(first.channel.as_ref())
            } else {
               channels.contains(first.channel.as_ref())
            }) {
               continue;
            }
            if !slot.has_pending()
               && conn.parser.wbuf.len().saturating_add(total)
                  <= crate::handler::conn::SUB_WRITE_BATCH_BYTES
            {
               conn.parser.wbuf.reserve(total);
               for entry in scratch.iter() {
                  conn.parser.wbuf.extend_from_slice(&entry.frame);
               }
            } else {
               let mut queued = slot.has_pending();
               for entry in scratch.iter() {
                  if !queued
                     && conn.parser.wbuf.len().saturating_add(entry.frame.len())
                        <= crate::handler::conn::SUB_WRITE_BATCH_BYTES
                  {
                     conn.parser.wbuf.extend_from_slice(&entry.frame);
                  } else {
                     slot.push_local(Arc::clone(&entry.frame));
                     queued = true;
                  }
               }
            }
            if seen.get(token).copied().unwrap_or(0) != fgen {
               if token < seen.len() {
                  seen[token] = fgen;
               }
               sub_dirty.push(token);
            }
         }
      } else {
         for entry in scratch.iter() {
            let subscriptions = if entry.is_pattern() {
               &local.patterns
            } else {
               &local.channels
            };
            let Some(members) = subscriptions.get(entry.channel.as_ref()) else {
               continue;
            };
            for member in members {
               if member.generation > entry.generation() {
                  continue;
               }
               let token = member.token;
               let Some(Some(conn)) = conns.get_mut(token) else {
                  continue;
               };
               let ConnMode::Subscribed {
                  slot,
                  channels,
                  patterns,
               } = &conn.mode
               else {
                  continue;
               };
               if !(if entry.is_pattern() {
                  patterns.contains(entry.channel.as_ref())
               } else {
                  channels.contains(entry.channel.as_ref())
               }) {
                  continue;
               }
               if !slot.has_pending()
                  && conn.parser.wbuf.len().saturating_add(entry.frame.len())
                     <= crate::handler::conn::SUB_WRITE_BATCH_BYTES
               {
                  conn.parser.wbuf.extend_from_slice(&entry.frame);
               } else {
                  slot.push_local(Arc::clone(&entry.frame));
               }
               if seen.get(token).copied().unwrap_or(0) != fgen {
                  if token < seen.len() {
                     seen[token] = fgen;
                  }
                  sub_dirty.push(token);
               }
            }
         }
      }
   });
   scratch.clear();
}

#[cfg(feature = "unix-sockets")]
static UNIX_LISTENERS: parking_lot::Mutex<
   Option<std::collections::HashMap<String, std::os::unix::net::UnixListener>>,
> = parking_lot::Mutex::new(None);

#[cfg(feature = "unix-sockets")]
fn parse_unix_path(bind: &str) -> Option<&str> {
   if let Some(path) = bind.strip_prefix("unix://") {
      Some(path)
   } else if let Some(path) = bind.strip_prefix("unix:") {
      Some(path)
   } else {
      None
   }
}

#[cfg(feature = "unix-sockets")]
fn make_unix_listener(path: &str, _worker_index: usize) -> UnixListener {
   use std::os::unix::net::UnixListener as StdUnixListener;

   let mut lock = UNIX_LISTENERS.lock();
   let map = lock.get_or_insert_with(std::collections::HashMap::new);

   if let Some(std_listener) = map.get(path) {
      let clone = std_listener
         .try_clone()
         .expect("failed to clone unix listener");
      return UnixListener::from_std(clone);
   }

   let _ = std::fs::remove_file(path);
   let std_listener = StdUnixListener::bind(path).unwrap_or_else(|e| {
      panic!("failed to bind unix socket at {path}: {e}");
   });
   std_listener.set_nonblocking(true).unwrap();
   let clone = std_listener
      .try_clone()
      .expect("failed to clone unix listener");

   map.insert(path.to_string(), std_listener);
   UnixListener::from_std(clone)
}

fn make_listener(port: u16, bind: &str, _worker_index: usize) -> Listener {
   #[cfg(not(feature = "unix-sockets"))]
   if bind.starts_with("unix:") || bind.starts_with("unix://") {
      panic!("Unix socket support is disabled; enable with feature 'unix-sockets'");
   }

   #[cfg(feature = "unix-sockets")]
   if let Some(path) = parse_unix_path(bind) {
      return Listener::Unix(make_unix_listener(path, _worker_index));
   }

   let addr: SocketAddr = format!("{}:{}", bind, port).parse().unwrap();
   Listener::Tcp(make_tcp_listener(addr))
}

fn make_tcp_listener(addr: SocketAddr) -> TcpListener {
   let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).unwrap();
   socket.set_reuse_address(true).unwrap();
   socket.set_reuse_port(true).unwrap();
   socket.set_nonblocking(true).unwrap();
   socket.bind(&addr.into()).unwrap();
   socket.listen(8192).unwrap();
   TcpListener::from_std(std::net::TcpListener::from(socket))
}

fn alloc_slot(
   free: &mut Vec<usize>,
   next_token: &mut usize,
   conns: &mut Vec<Option<Conn>>,
) -> usize {
   if let Some(id) = free.pop() {
      return id;
   }
   let id = *next_token;
   *next_token += 1;
   if id >= conns.len() {
      let previous = conns.capacity();
      conns.resize_with(id + 1, || None);
      if conns.capacity() != previous && previous * size_of::<Option<Conn>>() >= 32 * 1024 {
         // Reclaim large retired slot arrays during connection bursts;
         // ordinary request processing never pays for this collection.
         rust_zmalloc::purge();
      }
   }
   id
}

fn close_conn(conns: &mut [Option<Conn>], poll: &mut Poll, free: &mut Vec<usize>, id: usize) {
   if let Some(slot) = conns.get_mut(id)
      && let Some(mut conn) = slot.take()
   {
      let _ = poll.registry().deregister(&mut conn.stream);
      free.push(id);
   }
}

fn sync_write_interest(conns: &mut [Option<Conn>], poll: &mut Poll, id: usize) {
   let Some(Some(conn)) = conns.get_mut(id) else {
      return;
   };
   let wants_writable = conn.has_pending_write();
   if wants_writable == conn.writable_registered {
      return;
   }
   let interest = if wants_writable {
      Interest::READABLE | Interest::WRITABLE
   } else {
      Interest::READABLE
   };
   if poll
      .registry()
      .reregister(&mut conn.stream, Token(id), interest)
      .is_ok()
   {
      conn.writable_registered = wants_writable;
   }
}

#[cfg(test)]
mod pubsub_regressions {
   use super::*;

   fn with_subscriber(
      run: impl FnOnce(Conn<'_>, &PubSub, &Arc<WorkerNotifier>, &Arc<crate::pubsub::SubSlot>),
   ) {
      let store = Arc::new(Store::with_config_workers(1, usize::MAX, 2));
      let pubsub = PubSub::new();
      let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
      let client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
      let (_peer, _) = listener.accept().unwrap();
      client.set_nonblocking(true).unwrap();
      let stream = Stream::Tcp(mio::net::TcpStream::from_std(client));
      let poll = Poll::new().unwrap();
      let notifier = WorkerNotifier::new(
         Arc::new(Waker::new(poll.registry(), WAKER_TOKEN).unwrap()),
         0,
      );
      let slot = Arc::new(crate::pubsub::SubSlot::new(0, Arc::clone(&notifier)));
      let mut conn = Conn::new(stream, &store, &pubsub, 0, &notifier, None, 0);
      conn.mode = ConnMode::Subscribed {
         slot: Arc::clone(&slot),
         channels: Box::new(crate::handler::conn::Channels {
            keys: [Arc::<str>::from("a"), Arc::<str>::from("b")]
               .into_iter()
               .collect(),
            transaction: None,
         }),
         patterns: Default::default(),
      };
      for channel in ["a", "b"] {
         pubsub.subscribe(channel, Arc::clone(&slot));
         notifier.register_local(channel, 0);
      }
      run(conn, &pubsub, &notifier, &slot);
   }

   fn deliver(notifier: &Arc<WorkerNotifier>, conns: &mut [Option<Conn<'_>>]) {
      deliver_fanout(
         notifier,
         conns,
         &mut Vec::new(),
         &mut Vec::new(),
         &mut [0],
         1,
      );
   }

   #[test]
   fn small_new_frames_cannot_pass_an_older_queued_frame() {
      for mixed_channel in [false, true] {
         with_subscriber(|mut conn, pubsub, notifier, slot| {
            let prefix = vec![b'p'; crate::handler::conn::SUB_WRITE_BATCH_BYTES - 64];
            conn.parser.wbuf.extend_from_slice(&prefix);
            let large = "L".repeat(1024);
            let channel = if mixed_channel { "b" } else { "a" };
            pubsub.publish("a", &large);
            pubsub.publish(channel, "small");
            let mut conns = [Some(conn)];
            deliver(notifier, &mut conns);
            assert_eq!(conns[0].as_ref().unwrap().parser.wbuf, prefix);
            let mut actual = Vec::new();
            slot.drain_into(&mut actual);
            let expected = [
               crate::pubsub::frame::encode_message("a", &large).as_ref(),
               crate::pubsub::frame::encode_message(channel, "small").as_ref(),
            ]
            .concat();
            assert_eq!(actual, expected);
         });
      }
   }

   #[test]
   fn a_new_subscription_cannot_receive_an_old_queued_publication() {
      with_subscriber(|mut conn, pubsub, notifier, _old_slot| {
         assert_eq!(pubsub.publish("a", "old-secret"), 1);
         crate::handler::subscription::do_full_unsubscribe(&mut conn);
         let slot = Arc::new(crate::pubsub::SubSlot::new(0, Arc::clone(notifier)));
         pubsub.subscribe("a", Arc::clone(&slot));
         notifier.register_local("a", 0);
         conn.mode = ConnMode::Subscribed {
            slot: Arc::clone(&slot),
            channels: Box::new(crate::handler::conn::Channels {
               keys: [Arc::<str>::from("a")].into_iter().collect(),
               transaction: None,
            }),
            patterns: Default::default(),
         };
         let mut conns = [Some(conn)];
         deliver(notifier, &mut conns);
         assert!(conns[0].as_ref().unwrap().parser.wbuf.is_empty());
         assert!(!slot.has_pending());
      });
   }
   #[test]
   fn subscribed_commands_cannot_bypass_permissions_or_change_ping_framing() {
      with_subscriber(|mut conn, _pubsub, _notifier, _slot| {
         conn.store.set_string("secret", "old", 0);
         let commands: &[&[&[u8]]] = &[
            &[b"GET", b"secret"],
            &[b"SET", b"secret", b"changed"],
            &[b"PUBLISH", b"a", b"blocked"],
            &[b"PING", &[0, 255, 13, 10]],
            &[b"UNSUBSCRIBE", b"a", b"b"],
            &[b"GET", b"secret"],
         ];
         let mut wire = Vec::new();
         for command in commands {
            wire.extend_from_slice(format!("*{}\r\n", command.len()).as_bytes());
            for part in *command {
               wire.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
               wire.extend_from_slice(part);
               wire.extend_from_slice(b"\r\n");
            }
         }
         conn.parser.filled = wire.len();
         conn.parser.rbuf = wire;
         assert!(conn.process_buffered());
         let mut expected = b"-ERR Command not allowed in subscribed state\r\n".repeat(3);
         expected.extend_from_slice(b"*2\r\n$4\r\npong\r\n$4\r\n\0\xff\r\n\r\n");
         expected.extend_from_slice(b"*3\r\n$11\r\nunsubscribe\r\n$1\r\na\r\n:1\r\n*3\r\n$11\r\nunsubscribe\r\n$1\r\nb\r\n:0\r\n$3\r\nold\r\n");
         assert_eq!(conn.parser.wbuf, expected);
      });
   }
   #[test]
   fn direct_and_pattern_frames_keep_publication_order_for_one_subscriber() {
      for pattern in ["a", "a*"] {
         with_subscriber(|mut conn, pubsub, notifier, slot| {
            let name = pubsub.psubscribe_shared(pattern, Arc::clone(slot));
            let ConnMode::Subscribed { patterns, .. } = &mut conn.mode else {
               unreachable!()
            };
            patterns.insert(name);
            assert_eq!(pubsub.publish_batch("a", &["one", "two"]), 2);
            let mut conns = [Some(conn)];
            deliver(notifier, &mut conns);
            let mut expected = Vec::new();
            for message in ["one", "two"] {
               expected.extend_from_slice(&crate::pubsub::frame::encode_message("a", message));
               expected.extend_from_slice(&crate::pubsub::frame::encode_pmessage(
                  pattern, "a", message,
               ));
            }
            assert_eq!(conns[0].as_ref().unwrap().parser.wbuf, expected);
            assert!(!slot.has_pending());
         });
      }
   }
}
