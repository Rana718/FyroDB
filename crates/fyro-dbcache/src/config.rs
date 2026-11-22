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

   pub fn insert(&mut self, user: &str, password: &str) {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SslMode {
   #[default]
   Disable,
   Prefer,
   Require,
   VerifyCa,
   VerifyFull,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamTarget {
   pub host: String,
   pub port: u16,
   pub user: Option<String>,
   pub password: Option<String>,
   pub database: Option<String>,
   pub sslmode: SslMode,
}

impl UpstreamTarget {
   pub fn parse(input: &str) -> Self {
      let mut s = input.trim();
      let has_scheme = if let Some(stripped) = s.strip_prefix("postgresql://") {
         s = stripped;
         true
      } else if let Some(stripped) = s.strip_prefix("postgres://") {
         s = stripped;
         true
      } else {
         false
      };

      let mut sslmode = if has_scheme {
         SslMode::Prefer
      } else {
         SslMode::Disable
      };

      let (base, query) = match s.split_once('?') {
         Some((b, q)) => (b, Some(q)),
         None => (s, None),
      };

      if let Some(query) = query {
         for pair in query.split('&') {
            if let Some((k, v)) = pair.split_once('=')
               && k.eq_ignore_ascii_case("sslmode")
            {
               sslmode = match v.to_ascii_lowercase().as_str() {
                  "disable" => SslMode::Disable,
                  "prefer" | "allow" => SslMode::Prefer,
                  "require" => SslMode::Require,
                  "verify-ca" => SslMode::VerifyCa,
                  "verify-full" => SslMode::VerifyFull,
                  _ => SslMode::Prefer,
               };
            }
         }
      }

      let (auth_host, database) = match base.split_once('/') {
         Some((ah, db)) => {
            let db = percent_decode(db.trim());
            let db = if db.is_empty() { None } else { Some(db) };
            (ah, db)
         }
         None => (base, None),
      };

      let (user, password, host_port) = match auth_host.rsplit_once('@') {
         Some((user_pass, hp)) => {
            let (u, p) = match user_pass.split_once(':') {
               Some((u, p)) => (Some(percent_decode(u)), Some(percent_decode(p))),
               None => (Some(percent_decode(user_pass)), None),
            };
            (u, p, hp)
         }
         None => (None, None, auth_host),
      };

      let (host, port) = if host_port.starts_with('[') && host_port.contains(']') {
         let end = host_port.find(']').unwrap_or(host_port.len());
         let h = &host_port[1..end];
         let rest = &host_port[end + 1..];
         let p = rest
            .strip_prefix(':')
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(5432);
         (h.to_string(), p)
      } else if let Some((h, p)) = host_port.split_once(':') {
         let p = p.parse::<u16>().unwrap_or(5432);
         (h.to_string(), p)
      } else {
         let h = if host_port.is_empty() {
            "127.0.0.1"
         } else {
            host_port
         };
         (h.to_string(), 5432)
      };

      Self {
         host,
         port,
         user,
         password,
         database,
         sslmode,
      }
   }

   pub fn addr(&self) -> String {
      format!("{}:{}", self.host, self.port)
   }
}

fn percent_decode(s: &str) -> String {
   let mut bytes = Vec::with_capacity(s.len());
   let mut iter = s.as_bytes().iter();
   while let Some(&b) = iter.next() {
      if b == b'%' {
         let h1 = iter.next().copied();
         let h2 = iter.next().copied();
         if let (Some(h1), Some(h2)) = (h1, h2)
            && let Ok(val) = u8::from_str_radix(std::str::from_utf8(&[h1, h2]).unwrap_or(""), 16)
         {
            bytes.push(val);
            continue;
         }
      }
      bytes.push(b);
   }
   String::from_utf8_lossy(&bytes).into_owned()
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
   pub trust_auth: bool,
   // Unix socket used to broadcast invalidations to sibling processes.
   pub bus_path: Option<String>,
   // Number of mio worker threads for start_proxy_listener_mio.
   // 0 = auto (num_cpus / 2, min 1).
   pub proxy_workers: usize,
   // Concurrent client connections accepted; extra clients get PostgreSQL's
   // "too many clients" error. Zero means unlimited.
   pub max_connections: usize,
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
         pool_max_idle: 10,
         pool_idle_timeout_ms: 60_000,
         auth_users: AuthUsers::default(),
         trust_auth: false,
         bus_path: None,
         proxy_workers: 0,
         max_connections: DEFAULT_MAX_CONNECTIONS,
      }
   }
}

impl DbCacheConfig {
   /// Returns a client-ready connection URL for this proxy instance.
   pub fn client_url(&self) -> String {
      let port = self
         .listen_addr
         .rsplit_once(':')
         .map(|(_, p)| p)
         .unwrap_or("5433");
      if let Some(upstream) = &self.upstream_addr {
         let target = UpstreamTarget::parse(upstream);
         let user = target.user.as_deref().unwrap_or("postgres");
         let db = target.database.as_deref().unwrap_or("postgres");
         if let Some(pass) = &target.password {
            format!("postgres://{user}:{pass}@127.0.0.1:{port}/{db}?sslmode=disable")
         } else {
            format!("postgres://{user}@127.0.0.1:{port}/{db}?sslmode=disable")
         }
      } else {
         format!("postgres://postgres@127.0.0.1:{port}/postgres?sslmode=disable")
      }
   }

   // Builds configuration from environment variables.
   pub fn from_env() -> Self {
      Self::from_lookup(|name| env::var(name).ok())
   }

   /// Builds configuration from a variable lookup. Each setting accepts the
   /// documented `FYRODB_PROXY_*` name first and the original `FYRODB_DB_*`
   /// name second. The docs only ever listed the former while the code only
   /// read the latter, so documented deployments silently ran without the
   /// proxy (or with defaults); both now work, defaults are unchanged.
   pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Self {
      let var = |names: &[&str]| -> Option<String> {
         names
            .iter()
            .find_map(|name| get(name).filter(|value| !value.is_empty()))
      };
      let flag = |names: &[&str]| -> Option<bool> {
         var(names).map(|v| v == "1" || v.eq_ignore_ascii_case("true") || v == "yes")
      };
      let number = |names: &[&str]| -> Option<u64> { var(names).and_then(|v| v.parse().ok()) };

      let upstream = var(&["FYRODB_PROXY_UPSTREAM_URL", "FYRODB_DB_UPSTREAM"]);
      let enabled =
         upstream.is_some() || flag(&["FYRODB_PROXY_ENABLED", "FYRODB_DB_PROXY"]).unwrap_or(false);
      let cache_only = match var(&["FYRODB_PROXY_MODE"]) {
         Some(mode) => {
            mode.eq_ignore_ascii_case("cache-only") || mode.eq_ignore_ascii_case("cache_only")
         }
         None => flag(&["FYRODB_CACHE_ONLY"]).unwrap_or(false),
      };
      let mode = if cache_only {
         DbProxyMode::CacheOnly
      } else {
         DbProxyMode::Dual
      };

      let port = var(&["FYRODB_PROXY_PORT", "FYRODB_DB_PORT"])
         .and_then(|p| p.parse::<u16>().ok())
         .unwrap_or(5433);
      let bind =
         var(&["FYRODB_PROXY_BIND", "FYRODB_DB_BIND"]).unwrap_or_else(|| "0.0.0.0".to_string());
      let ttl = number(&["FYRODB_PROXY_CACHE_TTL_MS", "FYRODB_DB_TTL_MS"]).unwrap_or(60_000);
      let flush_on_start = match flag(&["FYRODB_PROXY_FLUSH_ON_START"]) {
         Some(flush) => flush,
         None => !flag(&["FYRODB_DB_KEEP_CACHE"]).unwrap_or(false),
      };
      let sync_interval = number(&["FYRODB_PROXY_SYNC_MS", "FYRODB_DB_SYNC_MS"]).unwrap_or(5_000);
      let max_keys =
         number(&["FYRODB_PROXY_CACHE_MAX_KEYS", "FYRODB_DB_MAX_KEYS"]).unwrap_or(100_000) as usize;
      let jitter = number(&["FYRODB_PROXY_CACHE_TTL_JITTER", "FYRODB_DB_TTL_JITTER_PCT"])
         .unwrap_or(10)
         .min(50);
      let pool_max_idle =
         number(&["FYRODB_PROXY_UPSTREAM_POOL", "FYRODB_DB_POOL_MAX_IDLE"]).unwrap_or(10) as usize;
      let pool_idle_timeout_ms =
         number(&["FYRODB_PROXY_UPSTREAM_IDLE_MS", "FYRODB_DB_POOL_IDLE_MS"]).unwrap_or(60_000);
      let key_prefix = var(&["FYRODB_PROXY_KEY_PREFIX"]).unwrap_or_else(|| "db:".to_string());

      let mut auth_users = AuthUsers::default();
      if let Some(ref target_str) = upstream {
         let target = UpstreamTarget::parse(target_str);
         if let (Some(u), Some(p)) = (&target.user, &target.password) {
            auth_users.insert(u, p);
         }
      }
      if let Some(spec) = var(&["FYRODB_PROXY_USERS", "FYRODB_DB_AUTH_USERS"]) {
         auth_users.parse_inline(&spec);
      }
      if let Some(path) = var(&["FYRODB_PROXY_USERS_FILE", "FYRODB_DB_AUTH_FILE"]) {
         auth_users.parse_file(&path);
      }
      let trust_auth = flag(&["FYRODB_PROXY_TRUST_AUTH", "FYRODB_DB_TRUST"]).unwrap_or(false);

      let bus_path = match get("FYRODB_PROXY_BUS").or_else(|| get("FYRODB_DB_BUS")) {
         Some(value)
            if value.is_empty()
               || value.eq_ignore_ascii_case("off")
               || value.eq_ignore_ascii_case("none") =>
         {
            None
         }
         Some(value) => Some(value),
         None => default_bus_path(),
      };

      Self {
         enabled,
         mode,
         upstream_addr: upstream,
         listen_addr: format!("{bind}:{port}"),
         default_ttl_ms: ttl,
         key_prefix,
         flush_on_start,
         sync_interval_ms: sync_interval,
         cache_max_keys: max_keys,
         ttl_jitter_pct: jitter,
         pool_max_idle,
         pool_idle_timeout_ms,
         auth_users,
         trust_auth,
         bus_path,
         proxy_workers: number(&["FYRODB_PROXY_WORKERS", "FYRODB_DB_PROXY_WORKERS"]).unwrap_or(0)
            as usize,
         max_connections: number(&["FYRODB_PROXY_MAX_CONNECTIONS", "FYRODB_DB_MAX_CONNECTIONS"])
            .unwrap_or(DEFAULT_MAX_CONNECTIONS as u64) as usize,
      }
   }
}

/// Default cap on concurrent proxy client connections; each one is served by
/// its own thread, so an unbounded flood meant unbounded threads.
pub const DEFAULT_MAX_CONNECTIONS: usize = 4_096;

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

#[cfg(test)]
mod tests {
   use super::*;

   fn config(vars: &[(&str, &str)]) -> DbCacheConfig {
      let vars: HashMap<String, String> = vars
         .iter()
         .map(|(k, v)| (k.to_string(), v.to_string()))
         .collect();
      DbCacheConfig::from_lookup(|name| vars.get(name).cloned())
   }

   #[test]
   fn documented_proxy_variables_configure_the_proxy() {
      let c = config(&[
         ("FYRODB_PROXY_ENABLED", "true"),
         ("FYRODB_PROXY_PORT", "5432"),
         ("FYRODB_PROXY_BIND", "127.0.0.1"),
         ("FYRODB_PROXY_MODE", "cache-only"),
         ("FYRODB_PROXY_CACHE_TTL_MS", "30000"),
         ("FYRODB_PROXY_CACHE_MAX_KEYS", "7"),
         ("FYRODB_PROXY_CACHE_TTL_JITTER", "5"),
         ("FYRODB_PROXY_UPSTREAM_POOL", "3"),
         ("FYRODB_PROXY_UPSTREAM_IDLE_MS", "1234"),
         ("FYRODB_PROXY_TRUST_AUTH", "true"),
         ("FYRODB_PROXY_USERS", "alice:pw1,bob:pw2"),
         ("FYRODB_PROXY_KEY_PREFIX", "pg:"),
         ("FYRODB_PROXY_FLUSH_ON_START", "false"),
         ("FYRODB_PROXY_MAX_CONNECTIONS", "99"),
         ("FYRODB_PROXY_BUS", "off"),
      ]);
      assert!(c.enabled);
      assert_eq!(c.listen_addr, "127.0.0.1:5432");
      assert_eq!(c.mode, DbProxyMode::CacheOnly);
      assert_eq!(c.default_ttl_ms, 30_000);
      assert_eq!(c.cache_max_keys, 7);
      assert_eq!(c.ttl_jitter_pct, 5);
      assert_eq!(c.pool_max_idle, 3);
      assert_eq!(c.pool_idle_timeout_ms, 1234);
      assert!(c.trust_auth);
      assert_eq!(c.auth_users.password("bob"), Some("pw2"));
      assert_eq!(c.key_prefix, "pg:");
      assert!(!c.flush_on_start);
      assert_eq!(c.max_connections, 99);
      assert_eq!(c.bus_path, None);

      let upstream = config(&[("FYRODB_PROXY_UPSTREAM_URL", "postgres://u:p@db:5432/app")]);
      assert!(upstream.enabled, "an upstream URL enables the proxy");
      assert_eq!(
         upstream.upstream_addr.as_deref(),
         Some("postgres://u:p@db:5432/app")
      );
      assert_eq!(upstream.mode, DbProxyMode::Dual);
   }

   #[test]
   fn legacy_names_still_work_and_documented_names_win() {
      let legacy = config(&[
         ("FYRODB_DB_PROXY", "1"),
         ("FYRODB_DB_PORT", "6000"),
         ("FYRODB_DB_KEEP_CACHE", "true"),
         ("FYRODB_CACHE_ONLY", "1"),
      ]);
      assert!(legacy.enabled);
      assert_eq!(legacy.listen_addr, "0.0.0.0:6000");
      assert!(!legacy.flush_on_start);
      assert_eq!(legacy.mode, DbProxyMode::CacheOnly);

      let both = config(&[("FYRODB_PROXY_PORT", "5432"), ("FYRODB_DB_PORT", "6000")]);
      assert_eq!(both.listen_addr, "0.0.0.0:5432");

      let defaults = config(&[]);
      assert!(!defaults.enabled);
      assert_eq!(defaults.listen_addr, "0.0.0.0:5433");
      assert!(defaults.flush_on_start);
      assert_eq!(defaults.pool_max_idle, 10);
      assert_eq!(defaults.max_connections, DEFAULT_MAX_CONNECTIONS);
   }

   #[test]
   fn test_upstream_target_parsing_neon_url() {
      let url = "postgresql://neondb_owner:npg_hVqDmCfj53rx@ep-solitary-water-b4uu6e7t.c-6.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require";
      let target = UpstreamTarget::parse(url);
      assert_eq!(
         target.host,
         "ep-solitary-water-b4uu6e7t.c-6.us-east-2.aws.neon.tech"
      );
      assert_eq!(target.port, 5432);
      assert_eq!(target.user.as_deref(), Some("neondb_owner"));
      assert_eq!(target.password.as_deref(), Some("npg_hVqDmCfj53rx"));
      assert_eq!(target.database.as_deref(), Some("neondb"));
      assert_eq!(target.sslmode, SslMode::Require);
      assert_eq!(
         target.addr(),
         "ep-solitary-water-b4uu6e7t.c-6.us-east-2.aws.neon.tech:5432"
      );
   }

   #[test]
   fn test_upstream_target_parsing_plain_and_encoded() {
      let plain = UpstreamTarget::parse("127.0.0.1:5432");
      assert_eq!(plain.host, "127.0.0.1");
      assert_eq!(plain.port, 5432);
      assert_eq!(plain.sslmode, SslMode::Disable);
      assert!(plain.user.is_none());

      let enc = UpstreamTarget::parse("postgresql://user%40domain:pass%3Aword@host:5433/db%20name");
      assert_eq!(enc.host, "host");
      assert_eq!(enc.port, 5433);
      assert_eq!(enc.user.as_deref(), Some("user@domain"));
      assert_eq!(enc.password.as_deref(), Some("pass:word"));
      assert_eq!(enc.database.as_deref(), Some("db name"));
      assert_eq!(enc.sslmode, SslMode::Prefer);
   }
}
