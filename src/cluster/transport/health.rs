use parking_lot::Mutex;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerState {
   Connecting,
   Connected,
   Healthy,
   Disconnected,
}

#[derive(Debug, Clone)]
pub struct PeerHealthSnapshot {
   pub state: PeerState,
   pub connected_at: Option<Instant>,
   pub last_pong_at: Option<Instant>,
   pub consecutive_failures: u64,
   pub generation: u64,
   /// Start of the current uninterrupted outage, including initial connection attempts.
   pub unavailable_since: Option<Instant>,
}

impl PeerHealthSnapshot {
   pub(super) fn unavailable_for(&self, now: Instant, timeout: Duration) -> bool {
      self
         .unavailable_since
         .is_some_and(|since| now.saturating_duration_since(since) >= timeout)
   }
}

#[derive(Clone)]
pub(crate) struct PeerHealth(Arc<Mutex<PeerHealthSnapshot>>);

impl PeerHealth {
   pub fn new() -> Self {
      Self(Arc::new(Mutex::new(PeerHealthSnapshot {
         state: PeerState::Disconnected,
         connected_at: None,
         last_pong_at: None,
         consecutive_failures: 0,
         generation: 0,
         unavailable_since: Some(Instant::now()),
      })))
   }

   pub fn connecting(&self) {
      self.0.lock().state = PeerState::Connecting;
   }
   pub fn connected(&self) -> u64 {
      let mut health = self.0.lock();
      health.generation = health.generation.wrapping_add(1);
      health.state = PeerState::Connected;
      health.connected_at = Some(Instant::now());
      health.last_pong_at = None;
      health.unavailable_since = None;
      health.generation
   }
   pub fn pong(&self, generation: u64) {
      let mut health = self.0.lock();
      if health.generation != generation
         || !matches!(health.state, PeerState::Connected | PeerState::Healthy)
      {
         return;
      }
      health.state = PeerState::Healthy;
      health.last_pong_at = Some(Instant::now());
      health.consecutive_failures = 0;
   }
   pub fn disconnected(&self, generation: Option<u64>) {
      let mut health = self.0.lock();
      if generation.is_some_and(|value| value != health.generation) {
         return;
      }
      health.unavailable_since.get_or_insert_with(Instant::now);
      health.state = PeerState::Disconnected;
      health.consecutive_failures = health.consecutive_failures.saturating_add(1);
   }
   pub fn snapshot(&self) -> PeerHealthSnapshot {
      self.0.lock().clone()
   }

   pub fn is_suspect(&self, timeout: Duration) -> bool {
      let health = self.0.lock();
      match health.state {
         PeerState::Disconnected | PeerState::Connecting => {
            health.unavailable_for(Instant::now(), timeout)
         }
         PeerState::Connected | PeerState::Healthy => health
            .last_pong_at
            .or(health.connected_at)
            .is_none_or(|last| last.elapsed() > timeout),
      }
   }
}

#[cfg(test)]
mod tests {
   use super::*;

   #[test]
   fn tracks_connection_and_failure_transitions() {
      let health = PeerHealth::new();
      health.connecting();
      assert_eq!(health.snapshot().state, PeerState::Connecting);
      let generation = health.connected();
      health.pong(generation);
      assert_eq!(health.snapshot().state, PeerState::Healthy);
      health.disconnected(Some(generation));
      assert_eq!(health.snapshot().consecutive_failures, 1);
   }

   #[test]
   fn startup_retries_share_one_suspicion_deadline() {
      let peer = PeerHealth::new();
      let start = peer.snapshot().unavailable_since.unwrap();
      let timeout = Duration::from_secs(6);
      assert!(!peer.is_suspect(timeout));
      peer.connecting();
      peer.disconnected(None);
      peer.connecting();
      let health = peer.snapshot();
      assert_eq!(health.unavailable_since, Some(start));
      assert!(!health.unavailable_for(start + timeout - Duration::from_nanos(1), timeout));
      assert!(health.unavailable_for(start + timeout, timeout));
   }

   #[test]
   fn reconnect_clears_old_evidence_and_the_next_outage_gets_a_new_deadline() {
      let peer = PeerHealth::new();
      let first = peer.connected();
      peer.pong(first);
      peer.disconnected(Some(first));
      peer.pong(first);
      assert_eq!(peer.snapshot().state, PeerState::Disconnected);
      let second = peer.connected();
      peer.disconnected(Some(first));
      assert_eq!(peer.snapshot().unavailable_since, None);
      assert_eq!(peer.snapshot().last_pong_at, None);
      let before_loss = Instant::now();
      peer.disconnected(Some(second));
      let lost = peer.snapshot().unavailable_since.unwrap();
      assert!(lost >= before_loss);
      peer.disconnected(None);
      assert_eq!(peer.snapshot().unavailable_since, Some(lost));
      assert!(
         !peer
            .snapshot()
            .unavailable_for(lost, Duration::from_secs(1))
      );
   }
}
