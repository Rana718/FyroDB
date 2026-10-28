use crate::query::compute_fingerprint;
use crate::upstream::UpstreamConnection;
use parking_lot::Mutex;
use std::time::{Duration, Instant};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct PoolKey {
   pub user: String,
   pub database: String,
   // Hash of startup parameters that cannot be restored after session reset.
   pub params: u64,
}

impl PoolKey {
   pub fn from_startup(params: &[(String, String)], user: &str, database: &str) -> Self {
      let mut joined = String::new();
      for (key, value) in params {
         // application_name is re-applied per checkout, so it never splits
         // otherwise-identical sessions across pool slots.
         if key.eq_ignore_ascii_case("user")
            || key.eq_ignore_ascii_case("database")
            || key.eq_ignore_ascii_case("application_name")
         {
            continue;
         }
         joined.push_str(key);
         joined.push('=');
         joined.push_str(value);
         joined.push('\u{1f}');
      }
      Self {
         user: user.to_string(),
         database: database.to_string(),
         params: compute_fingerprint(&joined),
      }
   }
}

pub fn application_name(params: &[(String, String)]) -> Option<&str> {
   params
      .iter()
      .find(|(key, _)| key.eq_ignore_ascii_case("application_name"))
      .map(|(_, value)| value.as_str())
}

// Validates that session parameters can be restored before pooling.
pub fn startup_allows_pooling(params: &[(String, String)]) -> bool {
   params.iter().all(|(key, _)| {
      key.eq_ignore_ascii_case("user")
         || key.eq_ignore_ascii_case("database")
         || key.eq_ignore_ascii_case("application_name")
   })
}

#[derive(Debug, Clone, Copy)]
pub struct PoolLimits {
   pub max_idle: usize,
   pub idle_timeout: Duration,
}

impl Default for PoolLimits {
   fn default() -> Self {
      Self {
         max_idle: 10,
         idle_timeout: Duration::from_secs(60),
      }
   }
}

struct IdleConnection {
   key: PoolKey,
   connection: UpstreamConnection,
   idle_since: Instant,
   required_password: bool,
}

// Keeps authenticated upstream sessions alive across client connections.
pub struct UpstreamPool {
   limits: PoolLimits,
   idle: Mutex<Vec<IdleConnection>>,
}

impl UpstreamPool {
   pub fn new(limits: PoolLimits) -> Self {
      Self {
         limits,
         idle: Mutex::new(Vec::new()),
      }
   }

   // Takes a matching idle connection (LIFO order to keep hot connections active),
   // pinging it before handing it out if idle for too long.
   pub fn checkout(
      &self,
      key: &PoolKey,
      require_passwordless: bool,
      application_name: Option<&str>,
   ) -> Option<UpstreamConnection> {
      loop {
         let now = Instant::now();
         let candidate = {
            let mut idle = self.idle.lock();
            let index = idle.iter().rposition(|slot| {
               &slot.key == key
                  && now.duration_since(slot.idle_since) <= self.limits.idle_timeout
                  && (!require_passwordless || !slot.required_password)
            })?;
            idle.swap_remove(index)
         };
         let idle_since = candidate.idle_since;
         let mut connection = candidate.connection;
         if idle_since.elapsed() > Duration::from_secs(10) && connection.ping().is_err() {
            continue;
         }
         if connection.apply_session_params(application_name).is_err() {
            continue;
         }
         return Some(connection);
      }
   }

   // Returns a session that was reset and is known to be idle.
   pub fn checkin(&self, key: PoolKey, connection: UpstreamConnection) {
      if self.limits.max_idle == 0 {
         return;
      }
      let required_password = connection.required_password();
      let mut idle = self.idle.lock();
      if idle.len() >= self.limits.max_idle {
         return;
      }
      idle.push(IdleConnection {
         key,
         connection,
         idle_since: Instant::now(),
         required_password,
      });
   }

   pub fn prune(&self) {
      self
         .idle
         .lock()
         .retain(|slot| slot.idle_since.elapsed() <= self.limits.idle_timeout);
   }

   pub fn idle_count(&self) -> usize {
      self.idle.lock().len()
   }
}

#[cfg(test)]
mod tests {
   use super::*;

   fn startup(entries: &[(&str, &str)]) -> Vec<(String, String)> {
      entries
         .iter()
         .map(|(key, value)| (key.to_string(), value.to_string()))
         .collect()
   }

   #[test]
   fn startup_parameters_decide_pooling() {
      assert!(startup_allows_pooling(&startup(&[
         ("user", "u"),
         ("database", "d")
      ])));
      assert!(startup_allows_pooling(&startup(&[
         ("user", "u"),
         ("database", "d"),
         ("application_name", "app")
      ])));
      assert!(!startup_allows_pooling(&startup(&[
         ("user", "u"),
         ("database", "d"),
         ("options", "-c search_path=other")
      ])));
      assert!(!startup_allows_pooling(&startup(&[
         ("user", "u"),
         ("client_encoding", "LATIN1")
      ])));
   }

   #[test]
   fn pool_key_reuses_sessions_across_application_names() {
      let params_a = startup(&[
         ("user", "u"),
         ("database", "d"),
         ("application_name", "first"),
      ]);
      let params_b = startup(&[
         ("user", "u"),
         ("database", "d"),
         ("application_name", "second"),
      ]);
      let key_a = PoolKey::from_startup(&params_a, "u", "d");
      let key_b = PoolKey::from_startup(&params_b, "u", "d");
      assert_eq!(key_a, key_b);
      assert_eq!(application_name(&params_a), Some("first"));
      assert_eq!(application_name(&params_b), Some("second"));

      let with_options = PoolKey::from_startup(
         &startup(&[
            ("user", "u"),
            ("database", "d"),
            ("options", "-c search_path=other"),
         ]),
         "u",
         "d",
      );
      assert_ne!(key_a, with_options);
   }
}
