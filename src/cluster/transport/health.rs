use std::sync::{Arc, Mutex};
use std::time::Instant;

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
        })))
    }

    pub fn connecting(&self) {
        self.0.lock().unwrap().state = PeerState::Connecting;
    }
    pub fn connected(&self) -> u64 {
        let mut health = self.0.lock().unwrap();
        health.generation = health.generation.wrapping_add(1);
        health.state = PeerState::Connected;
        health.connected_at = Some(Instant::now());
        health.generation
    }
    pub fn pong(&self, generation: u64) {
        let mut health = self.0.lock().unwrap();
        if health.generation != generation {
            return;
        }
        health.state = PeerState::Healthy;
        health.last_pong_at = Some(Instant::now());
        health.consecutive_failures = 0;
    }
    pub fn disconnected(&self, generation: Option<u64>) {
        let mut health = self.0.lock().unwrap();
        if generation.is_some_and(|value| value != health.generation) {
            return;
        }
        health.state = PeerState::Disconnected;
        health.consecutive_failures = health.consecutive_failures.saturating_add(1);
    }
    pub fn snapshot(&self) -> PeerHealthSnapshot {
        self.0.lock().unwrap().clone()
    }

    pub fn is_suspect(&self, timeout: std::time::Duration) -> bool {
        let health = self.0.lock().unwrap();
        match health.state {
            PeerState::Disconnected => true,
            PeerState::Connecting => health.connected_at.is_none(),
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
    fn disconnected_peer_is_suspect() {
        let health = PeerHealth::new();
        assert!(health.is_suspect(std::time::Duration::from_secs(1)));
        health.connected();
        assert!(!health.is_suspect(std::time::Duration::from_secs(1)));
    }
}
