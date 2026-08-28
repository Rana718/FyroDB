use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use super::Frame;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestError {
    Capacity,
    Duplicate,
    Unknown,
    Timeout,
    Disconnected,
    Closed,
}

struct State {
    pending: HashMap<u64, Option<Frame>>,
    closed: bool,
    failed: bool,
}

/// Bounded request/response correlation for multiplexed peer connections.
/// The registry never allocates beyond `capacity` outstanding requests.
#[derive(Clone)]
pub struct RequestRegistry {
    capacity: usize,
    state: Arc<(Mutex<State>, Condvar)>,
}

impl RequestRegistry {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Arc::new((
                Mutex::new(State {
                    pending: HashMap::new(),
                    closed: false,
                    failed: false,
                }),
                Condvar::new(),
            )),
        }
    }

    pub fn register(&self, request_id: u64) -> Result<(), RequestError> {
        let (lock, _) = &*self.state;
        let mut state = lock.lock().expect("request registry poisoned");
        if state.closed {
            return Err(RequestError::Closed);
        }
        state.failed = false;
        if state.pending.contains_key(&request_id) {
            return Err(RequestError::Duplicate);
        }
        if state.pending.len() >= self.capacity {
            return Err(RequestError::Capacity);
        }
        state.pending.insert(request_id, None);
        Ok(())
    }

    pub fn complete(&self, frame: Frame) -> Result<(), RequestError> {
        let (lock, wake) = &*self.state;
        let mut state = lock.lock().expect("request registry poisoned");
        let Some(slot) = state.pending.get_mut(&frame.request_id) else {
            return Err(RequestError::Unknown);
        };
        *slot = Some(frame);
        wake.notify_all();
        Ok(())
    }

    pub fn wait(&self, request_id: u64, timeout: Duration) -> Result<Frame, RequestError> {
        let (lock, wake) = &*self.state;
        let mut state = lock.lock().expect("request registry poisoned");
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if state.closed {
                return Err(RequestError::Closed);
            }
            if state.failed {
                return Err(RequestError::Disconnected);
            }
            let Some(slot) = state.pending.get(&request_id) else {
                return Err(RequestError::Unknown);
            };
            if slot.is_some() {
                return Ok(state
                    .pending
                    .remove(&request_id)
                    .and_then(|value| value)
                    .unwrap());
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                state.pending.remove(&request_id);
                return Err(RequestError::Timeout);
            }
            let (next, result) = wake
                .wait_timeout(state, remaining)
                .expect("request registry poisoned");
            state = next;
            if result.timed_out() {
                state.pending.remove(&request_id);
                return Err(RequestError::Timeout);
            }
        }
    }

    pub fn close(&self) {
        let (lock, wake) = &*self.state;
        let mut state = lock.lock().expect("request registry poisoned");
        state.closed = true;
        state.pending.clear();
        wake.notify_all();
    }

    pub fn fail_pending(&self) {
        let (lock, wake) = &*self.state;
        let mut state = lock.lock().expect("request registry poisoned");
        state.failed = true;
        state.pending.clear();
        wake.notify_all();
    }

    pub fn cancel(&self, request_id: u64) -> Result<(), RequestError> {
        let (lock, _) = &*self.state;
        let mut state = lock.lock().expect("request registry poisoned");
        state
            .pending
            .remove(&request_id)
            .map(|_| ())
            .ok_or(RequestError::Unknown)
    }

    pub fn len(&self) -> usize {
        self.state
            .0
            .lock()
            .expect("request registry poisoned")
            .pending
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{Frame, MessageType};

    fn response(id: u64) -> Frame {
        Frame {
            message_type: MessageType::CommandReply,
            flags: 0,
            request_id: id,
            source_id: 1,
            target_id: 2,
            epoch: 1,
            payload: b"ok".to_vec(),
        }
    }

    #[test]
    fn bounds_and_correlates_requests() {
        let registry = RequestRegistry::new(1);
        registry.register(7).unwrap();
        assert_eq!(registry.register(7), Err(RequestError::Duplicate));
        assert_eq!(registry.register(8), Err(RequestError::Capacity));
        registry.complete(response(7)).unwrap();
        assert_eq!(
            registry.wait(7, Duration::from_millis(10)).unwrap().payload,
            b"ok"
        );
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn close_wakes_waiters() {
        let registry = RequestRegistry::new(1);
        registry.register(1).unwrap();
        registry.close();
        assert_eq!(
            registry.wait(1, Duration::from_millis(10)),
            Err(RequestError::Closed)
        );
    }

    #[test]
    fn timeout_releases_capacity() {
        let registry = RequestRegistry::new(1);
        registry.register(1).unwrap();
        assert_eq!(
            registry.wait(1, Duration::from_millis(1)),
            Err(RequestError::Timeout)
        );
        registry.register(2).unwrap();
    }

    #[test]
    fn cancellation_releases_capacity() {
        let registry = RequestRegistry::new(1);
        registry.register(1).unwrap();
        registry.cancel(1).unwrap();
        registry.register(2).unwrap();
    }
}
