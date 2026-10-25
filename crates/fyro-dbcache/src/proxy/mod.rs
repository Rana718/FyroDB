mod session;

use crate::cache::DbCacheManager;
use crate::config::{DbCacheConfig, DbProxyMode};
use crate::pool::{PoolLimits, UpstreamPool};
use std::io;
use std::net::TcpListener;
use std::sync::Arc;
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

   thread::Builder::new()
      .name("fyrodb-dbproxy".into())
      .stack_size(256 * 1024)
      .spawn(move || {
         for stream in listener.incoming() {
            match stream {
               Ok(client) => {
                  let cfg = config.clone();
                  let mgr = Arc::clone(&cache_mgr);
                  let pool = Arc::clone(&pool);
                  let bus = bus.clone();
                  if let Err(e) = thread::Builder::new()
                     .name("fyrodb-dbconn".into())
                     .stack_size(session::CLIENT_STACK)
                     .spawn(move || {
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
