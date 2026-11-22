pub(crate) mod mio_worker;
mod session;
mod threads;

use crate::bus::InvalidationBus;
use crate::cache::DbCacheManager;
use crate::config::{DbCacheConfig, DbProxyMode};
use crate::pool::{PoolLimits, UpstreamPool};
use std::io;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

pub(crate) const CONTEXT_PROBE: &str = "SELECT current_setting('search_path', true), current_user, \
   current_setting('is_superuser', true), current_setting('row_security', true), \
   current_setting('default_transaction_isolation', true), \
   current_setting('timezone', true), current_setting('datestyle', true), \
   current_setting('intervalstyle', true), current_setting('extra_float_digits', true), \
   current_setting('client_encoding', true), current_setting('lc_numeric', true), \
   current_setting('default_text_search_config', true), \
   current_setting('bytea_output', true), \
   current_setting('standard_conforming_strings', true), \
   current_setting('server_encoding', true), current_database()";

pub(crate) const CATALOG_FUNCTIONS_SQL: &str = "SELECT n.nspname, p.proname FROM pg_proc p \
   JOIN pg_namespace n ON n.oid = p.pronamespace \
   WHERE p.provolatile IN ('s', 'v')";

pub(crate) const CATALOG_VIEWS_SQL: &str = "SELECT v.relname, pg_get_viewdef(v.oid), \
   d.refobjid::regclass::text FROM pg_rewrite r \
   JOIN pg_class v ON v.oid = r.ev_class AND v.relkind IN ('v', 'm') \
   JOIN pg_depend d ON d.objid = r.oid AND d.classid = 'pg_rewrite'::regclass \
      AND d.refclassid = 'pg_class'::regclass AND d.deptype = 'n' \
   WHERE r.rulename = '_RETURN'";

/// One admitted client connection; releases its slot when the session ends.
pub(crate) struct ConnectionPermit(Arc<AtomicUsize>);

impl Drop for ConnectionPermit {
   fn drop(&mut self) {
      self.0.fetch_sub(1, Ordering::Release);
   }
}

/// Admits a client under `max` concurrent connections (0 = unlimited). Every
/// upstream session runs on its own thread, so without a cap a connection
/// flood meant unbounded threads.
pub(crate) fn try_admit(active: &Arc<AtomicUsize>, max: usize) -> Option<ConnectionPermit> {
   let previous = active.fetch_add(1, Ordering::AcqRel);
   if max != 0 && previous >= max {
      active.fetch_sub(1, Ordering::Release);
      return None;
   }
   Some(ConnectionPermit(Arc::clone(active)))
}

/// Answers an over-limit client with PostgreSQL's own "too many clients"
/// error. Like PostgreSQL it first reads the startup packet (answering an SSL
/// request with 'N'), since closing a socket with unread input resets it and
/// the client would only see a broken connection. That wait happens on one
/// small rejecter thread so neither accept loop ever blocks; if it falls
/// behind during a flood, extra clients are just closed.
pub(crate) fn reject_too_many(client: TcpStream) {
   static REJECTER: std::sync::OnceLock<Option<std::sync::mpsc::SyncSender<TcpStream>>> =
      std::sync::OnceLock::new();
   let sender = REJECTER.get_or_init(|| {
      let (tx, rx) = std::sync::mpsc::sync_channel::<TcpStream>(64);
      thread::Builder::new()
         .name("fyrodb-dbreject".into())
         .stack_size(64 * 1024)
         .spawn(move || {
            for mut client in rx {
               answer_too_many(&mut client);
            }
         })
         .ok()
         .map(|_| tx)
   });
   if let Some(sender) = sender {
      let _ = sender.try_send(client);
   }
}

fn answer_too_many(client: &mut TcpStream) {
   use std::io::{Read, Write};
   let limit = Some(Duration::from_millis(250));
   if client.set_nonblocking(false).is_err()
      || client.set_read_timeout(limit).is_err()
      || client.set_write_timeout(limit).is_err()
   {
      return;
   }
   for _ in 0..2 {
      let mut header = [0u8; 8];
      if client.read_exact(&mut header).is_err() {
         break;
      }
      let len = i32::from_be_bytes([header[0], header[1], header[2], header[3]]);
      let code = i32::from_be_bytes([header[4], header[5], header[6], header[7]]);
      if !(8..=10_000).contains(&len) {
         break;
      }
      let mut rest = vec![0u8; len as usize - 8];
      if client.read_exact(&mut rest).is_err() {
         break;
      }
      if code == crate::protocol::SSL_REQUEST_CODE || code == crate::protocol::GSSENC_REQUEST_CODE {
         if client.write_all(b"N").is_err() {
            return;
         }
         continue;
      }
      break;
   }
   let _ =
      crate::protocol::write_fatal_response(client, "53300", "sorry, too many clients already");
   let _ = client.shutdown(std::net::Shutdown::Write);
}

// Spawns the background TCP listener for PostgreSQL wire proxy caching.
pub fn start_proxy_listener(
   config: DbCacheConfig,
   cache_mgr: Arc<DbCacheManager>,
) -> io::Result<thread::JoinHandle<()>> {
   let listener = TcpListener::bind(&config.listen_addr)?;
   let listen_addr = config.listen_addr.clone();

   let pool = Arc::new(UpstreamPool::new(PoolLimits {
      max_idle: config.pool_max_idle,
      idle_timeout: Duration::from_millis(config.pool_idle_timeout_ms),
   }));
   let bus = if config.mode == DbProxyMode::Dual {
      match config.bus_path.as_deref() {
         Some(path) => {
            match crate::bus::start(path, Arc::clone(&cache_mgr), config.key_prefix.clone()) {
               Ok(bus) => Some(bus),
               Err(error) => {
                  eprintln!("fyrodb-dbproxy: invalidation bus unavailable ({path}): {error}");
                  None
               }
            }
         }
         None => None,
      }
   } else {
      None
   };

   let janitor_pool = Arc::clone(&pool);
   let janitor_mgr = Arc::clone(&cache_mgr);
   let janitor_prefix = config.key_prefix.clone();
   let interval = Duration::from_millis(if config.pool_max_idle > 0 {
      (config.pool_idle_timeout_ms / 2).clamp(1_000, 5_000)
   } else {
      1_000
   });
   let _ = thread::Builder::new()
      .name("fyrodb-dbjanitor".into())
      .stack_size(128 * 1024)
      .spawn(move || {
         loop {
            thread::sleep(interval);
            if janitor_mgr.prune_due() {
               janitor_mgr.prune_expired(&janitor_prefix);
            }
            if janitor_pool.idle_count() > 0 {
               janitor_pool.prune();
               janitor_mgr.metrics().pool_idle.store(
                  janitor_pool.idle_count() as u64,
                  std::sync::atomic::Ordering::Relaxed,
               );
            }
         }
      });

   let active = Arc::new(AtomicUsize::new(0));
   let max_connections = config.max_connections;
   thread::Builder::new()
      .name("fyrodb-dbproxy".into())
      .stack_size(256 * 1024)
      .spawn(move || {
         for stream in listener.incoming() {
            match stream {
               Ok(client) => {
                  let Some(permit) = try_admit(&active, max_connections) else {
                     reject_too_many(client);
                     continue;
                  };
                  let cfg = config.clone();
                  let mgr = Arc::clone(&cache_mgr);
                  let pool = Arc::clone(&pool);
                  let bus = bus.clone();
                  if let Err(e) =
                     threads::spawn_session("fyrodb-dbconn", session::CLIENT_STACK, move || {
                        let _permit = permit;
                        if let Err(e) = session::handle_client(client, cfg, mgr, pool, bus)
                           && e.kind() != io::ErrorKind::UnexpectedEof
                        {
                           eprintln!("fyrodb-dbproxy client error: {e}");
                        }
                     })
                  {
                     eprintln!("fyrodb-dbproxy: spawn failed: {e}");
                  }
               }
               Err(e) => {
                  eprintln!("fyrodb-dbproxy accept error on {listen_addr}: {e}");
               }
            }
         }
      })
}

// Spawns N mio worker threads, each owning a SO_REUSEPORT listener on the
// same address. FyroDB's event loop is completely unaffected.
pub fn start_proxy_listener_mio(
   config: DbCacheConfig,
   cache_mgr: Arc<DbCacheManager>,
) -> io::Result<Vec<thread::JoinHandle<()>>> {
   use socket2::{Domain, Protocol, Socket, Type};

   let n_workers = if config.proxy_workers == 0 {
      std::thread::available_parallelism()
         .map(|n| n.get())
         .unwrap_or(4)
         .min(16)
   } else {
      config.proxy_workers
   };

   let addr: std::net::SocketAddr = config.listen_addr.parse().map_err(|e| {
      io::Error::new(
         io::ErrorKind::InvalidInput,
         format!("invalid listen addr: {e}"),
      )
   })?;

   let pool = Arc::new(UpstreamPool::new(PoolLimits {
      max_idle: config.pool_max_idle,
      idle_timeout: Duration::from_millis(config.pool_idle_timeout_ms),
   }));

   let bus: Option<Arc<InvalidationBus>> = if config.mode == DbProxyMode::Dual {
      match config.bus_path.as_deref() {
         Some(path) => {
            match crate::bus::start(path, Arc::clone(&cache_mgr), config.key_prefix.clone()) {
               Ok(b) => Some(b),
               Err(e) => {
                  eprintln!("fyrodb-dbproxy-mio: invalidation bus unavailable ({path}): {e}");
                  None
               }
            }
         }
         None => None,
      }
   } else {
      None
   };

   // One connection count across every worker of this listener.
   let active = Arc::new(AtomicUsize::new(0));
   let config = Arc::new(config);

   let mut handles = Vec::with_capacity(n_workers);
   for i in 0..n_workers {
      let sock = Socket::new(
         if addr.is_ipv6() {
            Domain::IPV6
         } else {
            Domain::IPV4
         },
         Type::STREAM,
         Some(Protocol::TCP),
      )?;
      sock.set_reuse_address(true)?;
      #[cfg(unix)]
      sock.set_reuse_port(true)?;
      sock.set_nonblocking(true)?;
      sock.bind(&addr.into())?;
      sock.listen(1024)?;

      let std_listener: TcpListener = sock.into();
      let mio_listener = mio::net::TcpListener::from_std(std_listener);
      let shared = mio_worker::WorkerShared {
         mgr: Arc::clone(&cache_mgr),
         config: Arc::clone(&config),
         pool: Arc::clone(&pool),
         bus: bus.clone(),
         active: Arc::clone(&active),
      };

      let handle = thread::Builder::new()
         .name(format!("fyrodb-mio-proxy-{i}"))
         .stack_size(256 * 1024)
         .spawn(move || mio_worker::run_proxy_worker(mio_listener, shared))?;
      handles.push(handle);
   }
   Ok(handles)
}
