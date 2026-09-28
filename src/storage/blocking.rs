//! Key waiters for blocking commands (BLPOP, BLMOVE, BZPOPMIN, XREAD BLOCK…).
//!
//! A connection that must wait registers one waiter per key it blocks on.
//! Writes that can satisfy a waiter (`push`, `zadd`, `xadd`, …) call
//! `signal(key)`; with nobody blocked that is a single relaxed load. A
//! signal wakes every waiter on the key through its worker's notifier; each
//! woken connection retries its command on its own worker, and the ones
//! that lose the race simply stay registered.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Delivers a retry to the worker that owns a parked connection.
pub trait Wake: Send + Sync {
    fn wake_retry(&self, token: usize, uid: u64);
}

#[derive(Clone)]
pub struct Waiter {
    pub wake: Arc<dyn Wake>,
    pub token: usize,
    pub uid: u64,
}

#[derive(Default)]
pub struct BlockRegistry {
    /// Registered (key, waiter) pairs; lets `signal` skip the lock.
    waiting: AtomicUsize,
    keys: Mutex<HashMap<String, Vec<Waiter>>>,
}

impl BlockRegistry {
    pub fn register(&self, keys: &[String], waiter: &Waiter) {
        let mut map = self.keys.lock();
        for k in keys {
            let list = map.entry(k.clone()).or_default();
            if !list.iter().any(|w| w.uid == waiter.uid) {
                list.push(waiter.clone());
                self.waiting.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn unregister(&self, keys: &[String], uid: u64) {
        let mut map = self.keys.lock();
        for k in keys {
            if let Some(list) = map.get_mut(k) {
                let before = list.len();
                list.retain(|w| w.uid != uid);
                self.waiting.fetch_sub(before - list.len(), Ordering::Relaxed);
                if list.is_empty() {
                    map.remove(k);
                }
            }
        }
    }

    /// A write made `key` possibly satisfiable: wake its waiters, oldest
    /// first (each worker retries in wake order).
    #[inline]
    pub fn signal(&self, key: &str) {
        if self.waiting.load(Ordering::Relaxed) == 0 {
            return;
        }
        self.signal_slow(key);
    }

    #[cold]
    fn signal_slow(&self, key: &str) {
        let map = self.keys.lock();
        if let Some(list) = map.get(key) {
            for w in list {
                w.wake.wake_retry(w.token, w.uid);
            }
        }
    }

    pub fn blocked_clients(&self) -> usize {
        let map = self.keys.lock();
        let mut uids: Vec<u64> = map.values().flatten().map(|w| w.uid).collect();
        uids.sort_unstable();
        uids.dedup();
        uids.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    #[derive(Default)]
    struct Rec(StdMutex<Vec<(usize, u64)>>);
    impl Wake for Rec {
        fn wake_retry(&self, token: usize, uid: u64) {
            self.0.lock().unwrap().push((token, uid));
        }
    }

    #[test]
    fn signal_reaches_registered_waiters_until_unregistered() {
        let reg = BlockRegistry::default();
        let rec = Arc::new(Rec::default());
        let w = Waiter { wake: rec.clone(), token: 7, uid: 42 };
        let keys = vec!["a".to_string(), "b".to_string()];
        reg.register(&keys, &w);
        reg.register(&keys, &w);
        assert_eq!(reg.blocked_clients(), 1);
        reg.signal("b");
        reg.signal("zzz");
        assert_eq!(*rec.0.lock().unwrap(), vec![(7, 42)]);
        reg.unregister(&keys, 42);
        assert_eq!(reg.waiting.load(Ordering::Relaxed), 0);
        reg.signal("a");
        assert_eq!(rec.0.lock().unwrap().len(), 1);
    }
}
