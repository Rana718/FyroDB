use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token, Waker};
use socket2::{Domain, Protocol, Socket, Type};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::handler::Conn;
use crate::handler::conn::ConnMode;
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
   let addr: SocketAddr = format!("{}:{}", bind, port).parse().unwrap();
   let mut listener = make_listener(addr);

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
   let mut fanout_seen: Vec<bool> = Vec::new();
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
         for id in 0..conns.len() {
            if let Some(Some(conn)) = conns.get_mut(id) {
               let _ = conn.do_write();
            }
         }
         return;
      }

      let mut timeout = if sub_dirty.is_empty() {
         None
      } else {
         Some(std::time::Duration::from_micros(50))
      };
      // Wake for the earliest blocked-command deadline.
      if let Some(deadline) = crate::handler::conn::park::next_deadline() {
         let until = deadline.saturating_duration_since(std::time::Instant::now());
         timeout = Some(timeout.map_or(until, |t| t.min(until)));
      }

      if let Some(window) = busy_window
         && last_active.elapsed() < window
      {
         timeout = Some(std::time::Duration::ZERO);
      }

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
                  Ok((mut stream, _)) => {
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

                     let sock_ref = socket2::SockRef::from(&stream);
                     let _ = sock_ref.set_tcp_nodelay(true);
                     let _ = sock_ref.set_recv_buffer_size(2 * 1024 * 1024);
                     let _ = sock_ref.set_send_buffer_size(2 * 1024 * 1024);
                     #[cfg(target_os = "linux")]
                     let _ = sock_ref.set_tcp_quickack(true);
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
               fanout_seen.resize(conns.len(), false);
               for seen in fanout_seen.iter_mut() {
                  *seen = false;
               }
               for &id in &sub_dirty {
                  if id < fanout_seen.len() {
                     fanout_seen[id] = true;
                  }
               }
               let pre_drain = sub_dirty.len();
               notifier.drain_pending_into(&mut sub_dirty);
               {
                  let mut w = pre_drain;
                  for r in pre_drain..sub_dirty.len() {
                     let id = sub_dirty[r];
                     if id < fanout_seen.len() && !fanout_seen[id] {
                        fanout_seen[id] = true;
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
               );
            }

            token => {
               let id = token.0;
               let mut close = false;
               if let Some(conn) = conns.get_mut(id).and_then(|s| s.as_mut()) {
                  if event.is_readable() && !conn.do_read() {
                     close = true;
                  }
                  // Retry WouldBlock writes when writable
                  if !close && event.is_writable() && !conn.do_write() {
                     close = true;
                  }
               }
               if close {
                  close_conn(&mut conns, &mut poll, &mut free, id);
               } else if conns
                  .get(id)
                  .and_then(|s| s.as_ref())
                  .is_some_and(|c| c.has_pending_write())
               {
                  dirty.push(id);
               } else if conns
                  .get(id)
                  .and_then(|s| s.as_ref())
                  .is_some_and(|c| c.writable_registered)
               {
                  sync_write_interest(&mut conns, &mut poll, id);
               }
            }
         }
      }

      if crate::handler::conn::park::next_deadline().is_some() {
         let now = std::time::Instant::now();
         crate::handler::conn::park::take_expired(now, &mut expired);
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
      if dirty.len() > 1 {
         dirty.sort_unstable();
         dirty.dedup();
      }
      #[cfg(feature = "io-uring")]
      if let Some(ring) = send_ring.as_mut() {
         sent.clear();
         for &id in &dirty {
            if let Some(Some(conn)) = conns.get_mut(id)
               && let Some((fd, ptr, len)) = conn.prepare_send()
            {
               ring.push(fd, ptr, len, id as u64, &mut sent);
            }
         }
         let mut w = 0;
         for r in 0..sub_dirty.len() {
            let id = sub_dirty[r];
            if let Some(Some(conn)) = conns.get_mut(id) {
               if is_slow_subscriber(conn) {
                  close_conn(&mut conns, &mut poll, &mut free, id);
                  continue;
               }
               if let Some((fd, ptr, len)) = conn.prepare_send() {
                  ring.push(fd, ptr, len, id as u64, &mut sent);
                  sub_dirty[w] = id;
                  w += 1;
               }
            }
         }
         sub_dirty.truncate(w);

         ring.submit_and_reap(&mut sent);
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
      slot.queue_len() > SLOW_SUB_MSG_CAP
   } else {
      false
   }
}

/// Frames append straight to wbuf (memcpy); a connection past the batch
/// cap spills into its slot queue (backlog drain, shedding intact).
fn deliver_fanout(
   notifier: &Arc<crate::pubsub::WorkerNotifier>,
   conns: &mut [Option<Conn>],
   sub_dirty: &mut Vec<usize>,
   scratch: &mut Vec<crate::pubsub::FanEntry>,
   seen: &mut [bool],
) {
   // Caller-owned scratch reused across wakeups; the subscription map is
   // borrowed under one lock for the whole batch.
   scratch.clear();
   notifier.drain_fanout(|entry| scratch.push(entry));
   if scratch.is_empty() {
      return;
   }
   notifier.with_local_subs(|subs| {
      for entry in scratch.iter() {
         let Some(tokens) = subs.get(entry.channel.as_ref()) else {
            continue;
         };
         for &token in tokens {
            let Some(Some(conn)) = conns.get_mut(token) else {
               continue;
            };
            // Conn's subscription set is the authority: tokens are reused after
            // close, so the local map alone could misroute.
            let subscribed = matches!(
                &conn.mode,
                crate::handler::conn::ConnMode::Subscribed { channels, .. }
                    if channels.contains(entry.channel.as_ref())
            );
            if !subscribed {
               continue;
            }
            if conn.parser.wbuf.len() + entry.frame.len()
               <= crate::handler::conn::SUB_WRITE_BATCH_BYTES
            {
               conn.parser.wbuf.extend_from_slice(&entry.frame);
            } else if let crate::handler::conn::ConnMode::Subscribed { slot, .. } = &conn.mode {
               slot.push(Arc::clone(&entry.frame));
            }
            if !seen[token] {
               seen[token] = true;
               sub_dirty.push(token);
            }
         }
      }
   });
}

fn make_listener(addr: SocketAddr) -> TcpListener {
   let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).unwrap();
   socket.set_reuse_address(true).unwrap();
   socket.set_reuse_port(true).unwrap();
   socket.set_nonblocking(true).unwrap();
   let _ = socket.set_recv_buffer_size(2 * 1024 * 1024);
   let _ = socket.set_send_buffer_size(2 * 1024 * 1024);
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
      conns.resize_with(id + 1, || None);
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
