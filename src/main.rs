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
use std::time::Duration;

#[global_allocator]
static GLOBAL: Zmalloc = Zmalloc;

fn main() {
    let config = Config::from_env();

    unsafe {
        let mut mask: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut mask);
        libc::sigaddset(&mut mask, libc::SIGTERM);
        libc::sigaddset(&mut mask, libc::SIGINT);
        libc::pthread_sigmask(libc::SIG_BLOCK, &mask, std::ptr::null_mut());
    }

    let workers = config.workers;
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

        let auth: Option<&str> = config.auth.as_deref();
        let port = config.port;
        let bind: &str = &config.bind;
        for worker_index in 0..workers {
            std::thread::Builder::new()
                .name("fyrodb-worker".into())
                .stack_size(128 * 1024)
                .spawn_scoped(scope, move || {
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
}

impl Config {
    fn from_env() -> Self {
        let workers = env_usize("FYRODB_WORKERS", 0);
        let workers = if workers == 0 {
            num_cpus::get()
        } else {
            workers
        };
        let shards = env_usize("FYRODB_SHARDS", 0);
        let shards = if shards == 0 {
            (workers * 4).next_power_of_two()
        } else {
            shards.next_power_of_two()
        };
        let max_keys = match env_usize("FYRODB_MAX_KEYS", 0) {
            0 => usize::MAX,
            configured => configured,
        };
        Config {
            port: env_u16("FYRODB_PORT", 8000),
            workers,
            shards,
            max_keys,
            max_clients: env_usize("FYRODB_MAX_CLIENTS", 10_000),
            rdb_path: env::var("FYRODB_RDB_PATH").unwrap_or_else(|_| "fyrodb.rdb".to_string()),
            rdb_interval: Duration::from_secs(env_u64("FYRODB_RDB_INTERVAL", 300)),
            auth: env::var("FYRODB_AUTH").ok().filter(|s| !s.is_empty()),
            bind: env::var("FYRODB_BIND").unwrap_or_else(|_| "0.0.0.0".to_string()),
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

fn expiry_loop(store: &Arc<Store>) {
    const SCAN_SLOTS_PER_TICK: usize = 262_144;
    /// Values rebuilt per defrag tick. Bounded so a large keyspace is covered
    /// over successive ticks instead of in one stop-the-shard pass.
    const DEFRAG_BUDGET: usize = 512;

    let shards = store.map_shard_count();
    let mut shard = 0usize;
    let mut slot = 0usize;
    let mut live_ttls = 0usize;
    let mut shard_removed = 0usize;
    let mut generation = store.ttl_generation();
    let mut capacities = vec![0usize; shards];
    let mut collect_tick = 0u8;
    let mut purge_tick = 0u8;
    let mut compact_tick = 0u16;
    let mut defrag_shard = 0usize;
    let mut defrag_slot = 0usize;
    let mut last_key_count = store.dbsize();
    loop {
        std::thread::sleep(Duration::from_secs(1));
        collect_tick += 1;
        purge_tick = purge_tick.saturating_add(1);
        compact_tick = compact_tick.saturating_add(1);
        if collect_tick >= 10 {
            collect_tick = 0;
            customhash::force_collect();
            rust_zmalloc::purge();
        }
        // Reclaim allocator pages on an existing maintenance cadence;
        // this is deliberately infrequent and does not affect hot
        // command paths.
        if purge_tick >= 60 {
            purge_tick = 0;
            let used = rust_zmalloc::used_memory();
            let rss = store::rss_bytes();
            if rss > used.saturating_add(used / 5) && rss.saturating_sub(used) >= 10 * 1024 * 1024 {
                // Active defrag cycle. Values are rebuilt under their
                // existing entry lock so lock-free readers never
                // observe a relocated entry address. The cursor keeps
                // each pass bounded regardless of keyspace size.
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
        if compact_tick >= 120 {
            compact_tick = 0;
            store.compact_underutilized();
            store::purge_allocator_if_fragmented();
        }
        if store.has_ttl_keys() {
            let (chunk_live_ttls, next_slot, capacity, removed) =
                store.cleanup_expired_shard(shard, slot, SCAN_SLOTS_PER_TICK);
            shard_removed += removed;
            if slot == 0 {
                capacities[shard] = capacity;
            } else if capacities[shard] != capacity {
                shard = 0;
                slot = 0;
                live_ttls = 0;
                generation = store.ttl_generation();
                continue;
            }
            live_ttls += chunk_live_ttls;
            if next_slot < capacity {
                slot = next_slot;
                continue;
            }

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
                store.finish_ttl_scan(generation, live_ttls);
                let cur_keys = store.dbsize();
                if cur_keys < last_key_count {
                    for s in 0..shards {
                        store.compact_shard(s);
                    }
                    customhash::force_collect();
                }
                last_key_count = cur_keys;
                live_ttls = 0;
                generation = store.ttl_generation();
            }
        } else {
            shard = 0;
            slot = 0;
            live_ttls = 0;
            generation = store.ttl_generation();
            let cur_keys = store.dbsize();
            if cur_keys < last_key_count {
                for s in 0..shards {
                    store.compact_shard(s);
                }
                customhash::force_collect();
            }
            last_key_count = cur_keys;
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

