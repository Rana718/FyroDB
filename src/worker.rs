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
    let mut events = Events::with_capacity(128);

    poll.registry()
        .register(&mut listener, LISTENER_TOKEN, Interest::READABLE)
        .unwrap();

    let waker = Arc::new(Waker::new(poll.registry(), WAKER_TOKEN).unwrap());
    let notifier = WorkerNotifier::new(waker, worker_index);

    // A Conn is large (socket, parser buffers, auth and pub/sub state). Reserving
    // 4096 slots per worker commits a sizeable idle allocation on high-core
    // machines. Grow with actual connections instead.
    let mut conns: Vec<Option<Conn>> = Vec::new();
    let mut next_token: usize = 1;
    let mut free: Vec<usize> = Vec::new();
    let mut dirty: Vec<usize> = Vec::with_capacity(32);
    let mut sub_dirty: Vec<usize> = Vec::with_capacity(16);
    let mut fanout_scratch: Vec<crate::pubsub::FanEntry> = Vec::new();
    let mut fanout_seen: Vec<bool> = Vec::new();

    loop {
        if SHUTDOWN.load(Ordering::Acquire) {
            for id in 0..conns.len() {
                if let Some(Some(conn)) = conns.get_mut(id) {
                    let _ = conn.do_write();
                }
            }
            return;
        }

        let timeout = if sub_dirty.is_empty() {
            None
        } else {
            Some(std::time::Duration::from_micros(50))
        };

        match poll.poll(&mut events, timeout) {
            Ok(_) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => continue,
        }

        tick_clock();

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

                            let _ = stream.set_nodelay(true);
                            let id = alloc_slot(&mut free, &mut next_token, &mut conns);
                            poll.registry()
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
                    notifier.drain_pending_into(&mut sub_dirty);
                    fanout_seen.resize(conns.len(), false);
                    for seen in fanout_seen.iter_mut() {
                        *seen = false;
                    }
                    deliver_fanout(
                        &notifier,
                        &mut conns,
                        &mut sub_dirty,
                        &mut fanout_scratch,
                        &mut fanout_seen,
                    );
                    sub_dirty.sort_unstable();
                    sub_dirty.dedup();
                }

                token => {
                    let id = token.0;
                    let mut close = false;
                    if let Some(conn) = conns.get_mut(id).and_then(|s| s.as_mut()) {
                        if event.is_readable() && !conn.do_read() {
                            close = true;
                        }
                        // This arm retries writes that previously hit WouldBlock;
                        // without it a client that stops reading mid-response
                        // never gets the rest.
                        if !close && event.is_writable() && !conn.do_write() {
                            close = true;
                        }
                    }
                    if close {
                        close_conn(&mut conns, &mut poll, &mut free, id);
                    } else {
                        dirty.push(id);
                    }
                }
            }
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

/// Copy each queued fan-out frame into this worker's subscriber reply
/// buffers.
///
/// Healthy connections get the frame appended straight to `wbuf` (plain
/// memcpy, no atomics). A connection whose buffer is already at the write
/// batch cap spills into its per-connection slot queue instead, where the
/// existing backlog drain and slow-subscriber shedding take over.
fn deliver_fanout(
    notifier: &Arc<crate::pubsub::WorkerNotifier>,
    conns: &mut [Option<Conn>],
    sub_dirty: &mut Vec<usize>,
    scratch: &mut Vec<crate::pubsub::FanEntry>,
    seen: &mut [bool],
) {
    // `scratch` is caller-owned and reused across wakeups: one allocation
    // per worker, not per batch. The local subscription map is borrowed
    // under a single lock for the whole batch, so draining is allocation
    // free per frame.
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
                // The conn's own subscription set is the delivery authority:
                // tokens are reused after close, so the local map alone could
                // route an in-flight frame to an unrelated new connection.
                let subscribed = matches!(
                    &conn.mode,
                    crate::handler::conn::ConnMode::Subscribed { channels, .. }
                        if channels.contains(entry.channel.as_ref())
                );
                if !subscribed {
                    continue;
                }
                // A batched entry carries a run of frames; the batch cap
                // applies per frame so overflow spills frame-by-frame into
                // the slot queue exactly like unbatched delivery.
                let mut spilled = false;
                for frame in entry.frames() {
                    if !spilled
                        && conn.parser.wbuf.len() + frame.len()
                            <= crate::handler::conn::SUB_WRITE_BATCH_BYTES
                    {
                        conn.parser.wbuf.extend_from_slice(frame);
                    } else if let crate::handler::conn::ConnMode::Subscribed { slot, .. } =
                        &conn.mode
                    {
                        slot.push(Arc::clone(frame));
                        spilled = true;
                    }
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
