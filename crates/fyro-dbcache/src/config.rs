use std::collections::HashMap;
use std::env;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbProxyMode {
   Dual,
   CacheOnly,
}

// Plaintext credentials the proxy authenticates frontends against and reuses
// for upstream logins. The debug representation never prints passwords.
#[derive(Clone, Default)]
pub struct AuthUsers {
   users: HashMap<String, String>,
}

impl AuthUsers {
   // Builds the credential map programmatically (tests, embedded servers).
   pub fn from_pairs<I, U, P>(users: I) -> Self
   where
      I: IntoIterator<Item = (U, P)>,
      U: AsRef<str>,
      P: AsRef<str>,
   {
      let mut auth = Self::default();
      for (user, password) in users {
         auth.insert(user.as_ref(), password.as_ref());
      }
      auth
   }

   pub fn password(&self, user: &str) -> Option<&str> {
      self.users.get(user).map(String::as_str)
   }

   pub fn len(&self) -> usize {
      self.users.len()
   }

   pub fn is_empty(&self) -> bool {
      self.users.is_empty()
   }

   fn insert(&mut self, user: &str, password: &str) {
      let user = user.trim();
      if !user.is_empty() {
         self
            .users
            .insert(user.to_string(), password.trim().to_string());
      }
   }

   fn parse_inline(&mut self, spec: &str) {
      for entry in spec.split([';', ',']) {
         if let Some((user, password)) = entry.split_once(':') {
            self.insert(user, password);
         }
      }
   }

   fn parse_file(&mut self, path: &str) {
      let Ok(contents) = std::fs::read_to_string(path) else {
         eprintln!("fyrodb: cannot read FYRODB_DB_AUTH_FILE={path}");
         return;
      };
      for line in contents.lines() {
         let line = line.trim();
         if line.is_empty() || line.starts_with('#') {
            continue;
         }
         if let Some((user, password)) = line.split_once(':') {
            self.insert(user, password);
         }
      }
   }
}

impl fmt::Debug for AuthUsers {
   fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
      let mut users: Vec<&String> = self.users.keys().collect();
      users.sort();
      f.debug_struct("AuthUsers").field("users", &users).finish()
   }
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
   // Authenticated upstream sessions kept for reuse between client
   // connections. Zero disables the pool.
   pub pool_max_idle: usize,
   pub pool_idle_timeout_ms: u64,
   // When non-empty, the proxy terminates frontend authentication with
   // SCRAM-SHA-256 and logs in to the origin with the same credentials.
   pub auth_users: AuthUsers,
   // Unix socket used to broadcast invalidations to sibling processes.
   pub bus_path: Option<String>,
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
         pool_max_idle: 8,
         pool_idle_timeout_ms: 30_000,
         auth_users: AuthUsers::default(),
         bus_path: None,
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
      let pool_max_idle = env::var("FYRODB_DB_POOL_MAX_IDLE")
         .ok()
         .and_then(|v| v.parse::<usize>().ok())
         .unwrap_or(8);
      let pool_idle_timeout_ms = env::var("FYRODB_DB_POOL_IDLE_MS")
         .ok()
         .and_then(|v| v.parse::<u64>().ok())
         .unwrap_or(30_000);

      let mut auth_users = AuthUsers::default();
      if let Ok(spec) = env::var("FYRODB_DB_AUTH_USERS")
         && !spec.is_empty()
      {
         auth_users.parse_inline(&spec);
      }
      if let Ok(path) = env::var("FYRODB_DB_AUTH_FILE")
         && !path.is_empty()
      {
         auth_users.parse_file(&path);
      }

      let bus_path = match env::var("FYRODB_DB_BUS") {
         Ok(value)
            if value.is_empty()
               || value.eq_ignore_ascii_case("off")
               || value.eq_ignore_ascii_case("none") =>
         {
            None
         }
         Ok(value) => Some(value),
         Err(_) => default_bus_path(),
      };

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
         pool_max_idle,
         pool_idle_timeout_ms,
         auth_users,
         bus_path,
      }
   }
}

fn default_bus_path() -> Option<String> {
   #[cfg(unix)]
   {
      let dir = env::var("XDG_RUNTIME_DIR")
         .ok()
         .filter(|value| !value.is_empty())
         .unwrap_or_else(|| "/tmp".to_string());
      Some(format!("{dir}/fyrodb-dbcache.bus"))
   }
   #[cfg(not(unix))]
   {
      None
   }
}
