use fyro_db::{
   pubsub::PubSub,
   storage::{
      rdb,
      store::{self, Store},
   },
   worker::{initiate_shutdown, run_worker, set_max_clients},
};
use rust_zmalloc::Zmalloc;
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[global_allocator]
static GLOBAL: Zmalloc = Zmalloc;

#[cfg(feature = "profiling")]
static PROFILER: std::sync::Mutex<Option<pprof::ProfilerGuard<'static>>> =
   std::sync::Mutex::new(None);

#[cfg(feature = "profiling")]
fn start_profiler() {
   if env::var_os("FYRODB_PPROF").is_some() {
      let guard = pprof::ProfilerGuardBuilder::default()
         .frequency(997)
         .blocklist(&["libc", "libgcc", "pthread", "vdso"])
         .build();
      *PROFILER.lock().unwrap() = guard.ok();
   }
}

#[cfg(feature = "profiling")]
fn write_profile() {
   use std::io::Write;
   let Some(guard) = PROFILER.lock().unwrap().take() else {
      return;
   };
   let Ok(path) = env::var("FYRODB_PPROF") else {
      return;
   };
   let Ok(report) = guard.report().build() else {
      return;
   };
   let Ok(mut f) = std::fs::File::create(&path) else {
      return;
   };
   for (frames, count) in report.data.iter() {
      let mut line = frames.thread_name.clone();
      for frame in frames.frames.iter().rev() {
         for sym in frame.iter().rev() {
            line.push(';');
            line.push_str(&sym.name());
            if let (Some(file), Some(no)) = (sym.filename.as_ref(), sym.lineno) {
               let file = file.to_string_lossy();
               let short = file.rsplit('/').take(2).collect::<Vec<_>>();
               line.push_str(&format!(
                  "@{}/{}:{no}",
                  short.get(1).unwrap_or(&""),
                  short[0]
               ));
            }
         }
      }
      let _ = writeln!(f, "{line} {count}");
   }
}

fn main() {
   if env::var("FYRODB_NULL_STORE").is_ok_and(|v| v == "1" || v == "true" || v == "yes") {
      fyro_db::handler::conn::set_null_store(true);
      println!("  NULL_STORE=enabled (TCP network & parser benchmark mode)");
   }
   #[cfg(feature = "bench-tools")]
   if env::var("FYRODB_RAW_ARRIVAL").is_ok_and(|v| v == "1" || v == "true" || v == "yes") {
      fyro_db::handler::conn::set_raw_arrival_only(true);
      println!("  RAW_ARRIVAL=enabled (pure network-to-parser ingress benchmark mode)");
   }
   let config = Config::from_env();
   rust_zmalloc::configure_allocator();
   #[cfg(feature = "profiling")]
   start_profiler();

   unsafe {
      let mut mask: libc::sigset_t = std::mem::zeroed();
      libc::sigemptyset(&mut mask);
      libc::sigaddset(&mut mask, libc::SIGTERM);
      libc::sigaddset(&mut mask, libc::SIGINT);
      libc::pthread_sigmask(libc::SIG_BLOCK, &mask, std::ptr::null_mut());
   }

   let workers = config.workers;
   let affinity = fyro_db::utils::affinity::from_env(workers);
   let search_cores = affinity.search.clone().or_else(|| affinity.workers.clone());
   fyro_db::search::configure_pool(Some(workers), search_cores);
   let store = Arc::new(Store::with_config_workers(
      config.shards,
      config.max_keys,
      config.workers,
   ));
   let pubsub = PubSub::new();

   set_max_clients(config.max_clients);

   if let Err(e) = rdb::load(&store, &config.rdb_path) {
      eprintln!("fyrodb: failed to load snapshot: {e}");
   }
   open_aof(&store, &config.rdb_path);
   // Vector fields without a persisted graph (or from a v2 snapshot)
   // rebuild in the background; FT.INFO reports progress meanwhile.
   store.search_state().rebuild_missing(&store);
   store.load_replication_metadata(&config.rdb_path);
   store.load_cluster_metadata(&config.rdb_path);

   let peer_manager = if store.cluster.enabled {
      let cluster_state = store.cluster_state();
      match fyro_db::cluster::start_listener(
         (*store.cluster).clone(),
         cluster_state.clone(),
         Arc::clone(&store),
      ) {
         Ok(_) => {
            println!(
               "  cluster=enabled node_id={} listen={}",
               store.cluster.local_id, store.cluster.listen_address
            );
            let manager = Arc::new(fyro_db::cluster::start_peer_manager(
               &store.cluster,
               cluster_state.clone(),
            ));
            fyro_db::cluster::start_health_monitor(
               (*store.cluster).clone(),
               Arc::clone(&manager),
               Arc::clone(&store),
               cluster_state,
            );
            Some(manager)
         }
         Err(error) => {
            eprintln!("fyrodb: failed to start cluster listener: {error}");
            std::process::exit(1);
         }
      }
   } else {
      None
   };

   if let (Some(manager), Some(log)) = (peer_manager.as_ref(), store.replication_coordinator()) {
      fyro_db::cluster::start_replication_streams(
         &store.cluster,
         Arc::clone(manager),
         Arc::clone(&store),
         log,
      );
   }

   println!(
      "fyrodb running on {}:{} ({workers} workers)",
      config.bind, config.port
   );
   println!(
      "  max_keys={} shards={} max_clients={} rdb_path={} rdb_interval={}s",
      if config.max_keys == usize::MAX {
         "unlimited".to_owned()
      } else {
         config.max_keys.to_string()
      },
      config.shards,
      config.max_clients,
      config.rdb_path,
      config.rdb_interval.as_secs()
   );
   if config.auth.is_some() {
      println!("  auth=enabled");
   }

   if config.dbcache.enabled {
      let storage: Arc<dyn fyro_dbcache::DbCacheStorage> = store.clone();
      let cache_mgr = Arc::new(fyro_dbcache::DbCacheManager::with_limits(
         storage,
         fyro_dbcache::CacheLimits {
            max_keys: config.dbcache.cache_max_keys,
            ttl_jitter_pct: config.dbcache.ttl_jitter_pct,
            sync_interval_ms: config.dbcache.sync_interval_ms,
         },
      ));
      store.install_dbcache_manager(Arc::clone(&cache_mgr));
      if config.dbcache.flush_on_start {
         let dropped = cache_mgr.flush_all(&config.dbcache.key_prefix);
         if dropped > 0 {
            println!("  dbcache=flushed {dropped} persisted entries");
         }
      } else {
         let rebuilt = cache_mgr.rebuild_tracker(&config.dbcache.key_prefix);
         if rebuilt > 0 {
            println!("  dbcache=rebuilt invalidation index ({rebuilt} entries)");
         }
      }
      if let Err(e) = fyro_dbcache::start_proxy_listener(config.dbcache.clone(), cache_mgr) {
         eprintln!("fyrodb: failed to start dbcache proxy: {e}");
      } else {
         println!(
            "  dbcache=enabled listen={} upstream={:?} mode={:?}",
            config.dbcache.listen_addr, config.dbcache.upstream_addr, config.dbcache.mode
         );
         println!(
            "  dbcache pool_idle_max={} pool_idle={}ms auth_users={} bus={}",
            config.dbcache.pool_max_idle,
            config.dbcache.pool_idle_timeout_ms,
            config.dbcache.auth_users.len(),
            config.dbcache.bus_path.as_deref().unwrap_or("off"),
         );
      }
   }

   std::thread::scope(|scope| {
      let store_ref: &Arc<Store> = &store;
      let pubsub_ref: &PubSub = &pubsub;

      std::thread::Builder::new()
         .name("fyrodb-expiry".into())
         .stack_size(64 * 1024)
         .spawn_scoped(scope, || expiry_loop(store_ref))
         .expect("failed to spawn expiry thread");
      std::thread::Builder::new()
         .name("fyrodb-rdb-saver".into())
         .stack_size(64 * 1024)
         .spawn_scoped(scope, || {
            rdb::background_save_loop(store_ref, &config.rdb_path, config.rdb_interval)
         })
         .expect("failed to spawn RDB saver thread");
      std::thread::Builder::new()
         .name("fyrodb-signal".into())
         .stack_size(64 * 1024)
         .spawn_scoped(scope, || signal_loop(store_ref, &config.rdb_path))
         .expect("failed to spawn signal thread");

      if config.dbcache.mode == fyro_dbcache::DbProxyMode::CacheOnly {
         println!(
            "  cache_only=enabled (Redis port {} suspended)",
            config.port
         );
         loop {
            std::thread::sleep(Duration::from_secs(3600));
         }
      }

      let auth: Option<&str> = config.auth.as_deref();
      let port = config.port;
      let bind: &str = &config.bind;
      let worker_cores = affinity.workers.clone();
      if let Some(c) = &worker_cores {
         println!("  worker_cores={c:?} search_cores={:?}", affinity.search);
      }
      for worker_index in 0..workers {
         let core = worker_cores.as_ref().map(|c| c[worker_index % c.len()]);
         // Debug builds use much larger frames than release, so keep a safety
         // margin unless the operator sizes the stack explicitly.
         let stack_size = env::var("FYRODB_WORKER_STACK_KB")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .map(|kb| kb.saturating_mul(1024))
            .unwrap_or(if cfg!(debug_assertions) {
               1024 * 1024
            } else {
               128 * 1024
            });
         std::thread::Builder::new()
            .name("fyrodb-worker".into())
            .stack_size(stack_size)
            .spawn_scoped(scope, move || {
               if let Some(core) = core {
                  fyro_db::utils::affinity::pin_current(core);
               }
               run_worker(store_ref, pubsub_ref, port, bind, auth, worker_index)
            })
            .expect("failed to spawn worker");
      }
   });
}

struct Config {
   port: u16,
   workers: usize,
   shards: usize,
   max_keys: usize,
   max_clients: usize,
   rdb_path: String,
   rdb_interval: Duration,
   auth: Option<String>,
   bind: String,
   dbcache: fyro_dbcache::DbCacheConfig,
}

impl Config {
   fn from_env() -> Self {
      let mut workers = env_usize("FYRODB_WORKERS", 0);
      let mut port = env_u16("FYRODB_PORT", 8000);
      let mut auth = env::var("FYRODB_AUTH").ok().filter(|s| !s.is_empty());
      let mut dbcache = fyro_dbcache::DbCacheConfig::from_env();
      let args: Vec<String> = env::args().collect();
      let mut i = 1;
      while i < args.len() {
         match args[i].as_str() {
            "--workers" | "-w" if i + 1 < args.len() => {
               if let Ok(w) = args[i + 1].parse::<usize>() {
                  workers = w;
               }
               i += 1;
            }
            "--port" | "-p" if i + 1 < args.len() => {
               if let Ok(p) = args[i + 1].parse::<u16>() {
                  port = p;
               }
               i += 1;
            }
            "--requirepass" | "--auth" | "-a" if i + 1 < args.len() => {
               auth = Some(args[i + 1].clone());
               i += 1;
            }
            "--db-upstream" if i + 1 < args.len() => {
               dbcache.enabled = true;
               dbcache.upstream_addr = Some(args[i + 1].clone());
               i += 1;
            }
            "--cache-only" => {
               dbcache.enabled = true;
               dbcache.mode = fyro_dbcache::DbProxyMode::CacheOnly;
            }
            "--db-port" if i + 1 < args.len() => {
               dbcache.enabled = true;
               if let Ok(p) = args[i + 1].parse::<u16>() {
                  let bind = env::var("FYRODB_DB_BIND").unwrap_or_else(|_| "0.0.0.0".to_string());
                  dbcache.listen_addr = format!("{bind}:{p}");
               }
               i += 1;
            }
            _ => {}
         }
         i += 1;
      }
      let workers = if workers == 0 {
         fyro_vector::pool::allowed_cores().len().max(1)
      } else {
         workers
      };
      let shards = env_usize("FYRODB_SHARDS", 0);
      let shards = if shards == 0 {
         (workers * 4).max(128).next_power_of_two()
      } else {
         shards.next_power_of_two()
      };
      let max_keys = match env_usize("FYRODB_MAX_KEYS", 0) {
         0 => usize::MAX,
         configured => configured,
      };

      Config {
         port,
         workers,
         shards,
         max_keys,
         max_clients: env_usize("FYRODB_MAX_CLIENTS", 10_000),
         rdb_path: env::var("FYRODB_RDB_PATH").unwrap_or_else(|_| "fyrodb.rdb".to_string()),
         rdb_interval: Duration::from_secs(env_u64("FYRODB_RDB_INTERVAL", 300)),
         auth,
         bind: env::var("FYRODB_BIND").unwrap_or_else(|_| "0.0.0.0".to_string()),
         dbcache,
      }
   }
}

fn env_u16(key: &str, default: u16) -> u16 {
   env::var(key)
      .ok()
      .and_then(|v| v.parse().ok())
      .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
   env::var(key)
      .ok()
      .and_then(|v| v.parse().ok())
      .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
   env::var(key)
      .ok()
      .and_then(|v| v.parse().ok())
      .unwrap_or(default)
}

fn open_aof(store: &Store, rdb_path: &str) {
   use fyro_db::storage::persistence::aof;
   let on = env::var("FYRODB_AOF").is_ok_and(|v| matches!(v.as_str(), "1" | "yes" | "true"));
   if !on {
      return;
   }
   let dir = env::var("FYRODB_AOF_DIR")
      .map(std::path::PathBuf::from)
      .unwrap_or_else(|_| {
         std::path::Path::new(rdb_path)
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(std::path::Path::new("."))
            .join("aof")
      });
   let fsync = match env::var("FYRODB_AOF_FSYNC").as_deref() {
      Ok("always") => aof::Fsync::Always,
      Ok("no") => aof::Fsync::No,
      _ => aof::Fsync::EverySec,
   };
   match aof::open(store, &dir, fsync) {
      Ok(n) => eprintln!(
         "fyrodb: aof enabled at {} ({fsync:?}), replayed {n} records",
         dir.display()
      ),
      Err(e) => {
         eprintln!("fyrodb: failed to open aof at {}: {e}", dir.display());
         std::process::exit(1);
      }
   }
}

fn expiry_loop(store: &Arc<Store>) {
   const TICK: Duration = Duration::from_millis(100);
   const TICK_BUDGET: Duration = Duration::from_millis(25);
   const CHUNK_SLOTS: usize = 32_768;
   const DEFRAG_BUDGET: usize = 512;

   let shards = store.map_shard_count();
   let mut shard = 0usize;
   let mut slot = 0usize;
   let mut live_ttls = 0usize;
   let mut shard_removed = 0usize;
   let mut generation = store.ttl_generation();
   let mut capacities = vec![0usize; shards];
   let mut defrag_shard = 0usize;
   let mut defrag_slot = 0usize;
   let mut last_key_count = store.dbsize();
   let mut tick = 0u64;
   let mut rest = Duration::ZERO;
   let mut rest_until = Instant::now();
   let mut cycle_seen = 0usize;
   let mut cycle_removed = 0usize;
   let compact_if_shrunk = |last: &mut usize| {
      let cur = store.dbsize();
      if cur < *last {
         for s in 0..shards {
            store.compact_shard(s);
         }
         customhash::force_collect();
      }
      *last = cur;
   };
   loop {
      std::thread::sleep(TICK);
      tick += 1;
      if tick.is_multiple_of(100) {
         customhash::force_collect();
         rust_zmalloc::purge();
      }
      if tick.is_multiple_of(600) {
         let used = rust_zmalloc::used_memory();
         let rss = store::rss_bytes();
         if rss > used.saturating_add(used / 5) && rss.saturating_sub(used) >= 10 * 1024 * 1024 {
            let (next_slot, capacity, rebuilt) =
               store.defragment_shard_range(defrag_shard, defrag_slot, DEFRAG_BUDGET);
            if next_slot >= capacity {
               defrag_slot = 0;
               defrag_shard = (defrag_shard + 1) % shards;
            } else {
               defrag_slot = next_slot;
            }
            if rebuilt != 0 {
               customhash::force_collect_quiescent();
            }
         }
         store::purge_allocator_if_fragmented();
      }
      if tick.is_multiple_of(1200) {
         store.compact_underutilized();
         store::purge_allocator_if_fragmented();
      }
      if !store.has_ttl_keys() {
         shard = 0;
         slot = 0;
         live_ttls = 0;
         generation = store.ttl_generation();
         if tick.is_multiple_of(10) {
            compact_if_shrunk(&mut last_key_count);
         }
         continue;
      }
      if Instant::now() < rest_until {
         continue;
      }
      let started = Instant::now();
      loop {
         let (chunk_live, next_slot, capacity, removed) =
            store.cleanup_expired_shard(shard, slot, CHUNK_SLOTS);
         shard_removed += removed;
         if slot == 0 {
            capacities[shard] = capacity;
         } else if capacities[shard] != capacity {
            shard = 0;
            slot = 0;
            live_ttls = 0;
            generation = store.ttl_generation();
            break;
         }
         live_ttls += chunk_live;
         cycle_seen += chunk_live + removed;
         cycle_removed += removed;
         if next_slot < capacity {
            slot = next_slot;
         } else {
            if shard_removed != 0 {
               store.compact_shard(shard);
               customhash::force_collect_quiescent();
               store::purge_allocator_if_fragmented();
               shard_removed = 0;
            }
            slot = 0;
            shard += 1;
            if shard >= shards {
               shard = 0;
               if cycle_removed * 100 < cycle_seen {
                  rest = (rest * 2).clamp(Duration::from_secs(1), Duration::from_secs(8));
                  rest_until = Instant::now() + rest;
               } else {
                  rest = Duration::ZERO;
               }
               cycle_seen = 0;
               cycle_removed = 0;
               store.finish_ttl_scan(generation, live_ttls);
               compact_if_shrunk(&mut last_key_count);
               live_ttls = 0;
               generation = store.ttl_generation();
            }
         }
         let seen = chunk_live + removed;
         let hot = seen > 0 && removed * 10 > seen;
         if hot {
            rest = Duration::ZERO;
            rest_until = Instant::now();
         }
         if !hot || started.elapsed() >= TICK_BUDGET || !store.has_ttl_keys() {
            break;
         }
      }
   }
}

fn signal_loop(store: &Arc<Store>, rdb_path: &str) {
   let mut sig = 0i32;
   unsafe {
      let mut mask: libc::sigset_t = std::mem::zeroed();
      libc::sigemptyset(&mut mask);
      libc::sigaddset(&mut mask, libc::SIGTERM);
      libc::sigaddset(&mut mask, libc::SIGINT);
      libc::sigwait(&mask, &mut sig);
   }
   eprintln!("fyrodb: received signal {sig}, shutting down...");
   #[cfg(feature = "profiling")]
   let _ = std::thread::Builder::new()
      .stack_size(64 << 20)
      .spawn(write_profile)
      .map(|h| h.join());
   initiate_shutdown();
   std::thread::sleep(Duration::from_millis(100));
   if let Err(e) = rdb::save(store, rdb_path) {
      eprintln!("fyrodb: save failed: {e}");
   }
   eprintln!("fyrodb: shutdown complete");
   std::process::exit(0);
}

mod libc {
   pub use ::libc::*;
}
