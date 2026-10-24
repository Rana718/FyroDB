use std::env;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbProxyMode {
   Dual,
   CacheOnly,
}

#[derive(Debug, Clone)]
pub struct DbCacheConfig {
   pub enabled: bool,
   pub mode: DbProxyMode,
   pub upstream_addr: Option<String>,
   pub listen_addr: String,
   pub default_ttl_ms: u64,
   pub key_prefix: String,
   // Drops persisted query results on startup. Keeping them is opt-in because
   // writes applied while FyroDB was down cannot be tracked.
   pub flush_on_start: bool,
   // Poll interval for writes that bypass the proxy, detected through the
   // pg_stat counters. Zero disables polling.
   pub sync_interval_ms: u64,
   // Hard cap on cached query results. Zero means unlimited.
   pub cache_max_keys: usize,
   // Randomizes entry lifetimes by up to +/- this percentage so a burst of
   // fills does not expire in lockstep.
   pub ttl_jitter_pct: u64,
}

impl Default for DbCacheConfig {
   fn default() -> Self {
      Self {
         enabled: false,
         mode: DbProxyMode::Dual,
         upstream_addr: None,
         listen_addr: "0.0.0.0:5433".to_string(),
         default_ttl_ms: 60_000,
         key_prefix: "db:".to_string(),
         flush_on_start: true,
         sync_interval_ms: 5_000,
         cache_max_keys: 100_000,
         ttl_jitter_pct: 10,
      }
   }
}

impl DbCacheConfig {
   // Builds configuration from environment variables.
   pub fn from_env() -> Self {
      let upstream = env::var("FYRODB_DB_UPSTREAM")
         .ok()
         .filter(|s| !s.is_empty());
      let enabled =
         upstream.is_some() || env::var("FYRODB_DB_PROXY").is_ok_and(|v| v == "1" || v == "true");
      let cache_only = env::var("FYRODB_CACHE_ONLY").is_ok_and(|v| v == "1" || v == "true");

      let mode = if cache_only {
         DbProxyMode::CacheOnly
      } else {
         DbProxyMode::Dual
      };

      let port = env::var("FYRODB_DB_PORT")
         .ok()
         .and_then(|p| p.parse::<u16>().ok())
         .unwrap_or(5433);

      let bind = env::var("FYRODB_DB_BIND").unwrap_or_else(|_| "0.0.0.0".to_string());
      let ttl = env::var("FYRODB_DB_TTL_MS")
         .ok()
         .and_then(|t| t.parse::<u64>().ok())
         .unwrap_or(60_000);
      let keep_cache =
         env::var("FYRODB_DB_KEEP_CACHE").is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));
      let sync_interval = env::var("FYRODB_DB_SYNC_MS")
         .ok()
         .and_then(|v| v.parse::<u64>().ok())
         .unwrap_or(5_000);
      let max_keys = env::var("FYRODB_DB_MAX_KEYS")
         .ok()
         .and_then(|v| v.parse::<usize>().ok())
         .unwrap_or(100_000);
      let jitter = env::var("FYRODB_DB_TTL_JITTER_PCT")
         .ok()
         .and_then(|v| v.parse::<u64>().ok())
         .unwrap_or(10)
         .min(50);

      Self {
         enabled,
         mode,
         upstream_addr: upstream,
         listen_addr: format!("{bind}:{port}"),
         default_ttl_ms: ttl,
         key_prefix: "db:".to_string(),
         flush_on_start: !keep_cache,
         sync_interval_ms: sync_interval,
         cache_max_keys: max_keys,
         ttl_jitter_pct: jitter,
      }
   }
}
