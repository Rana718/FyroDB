use crate::shard::{Entry, STATE_OCCUPIED, STATE_SNAP};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

pub type SnapSink<V> = Arc<dyn Fn(&str, &V) + Send + Sync>;

pub(crate) struct SnapCtl<V> {
    active: AtomicBool,
    parity: AtomicU64,
    sink: RwLock<Option<SnapSink<V>>>,
}

impl<V> SnapCtl<V> {
    pub(crate) fn new() -> Self {
        Self {
            active: AtomicBool::new(false),
            parity: AtomicU64::new(0),
            sink: RwLock::new(None),
        }
    }

    #[inline(always)]
    pub(crate) fn fresh_state(&self) -> u64 {
        STATE_OCCUPIED | self.parity.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub(crate) fn parity(&self) -> u64 {
        self.parity.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub(crate) fn before_write(&self, e: &Entry<V>) {
        if self.active.load(Ordering::Acquire) {
            self.capture(e);
        }
    }

    #[cold]
    #[inline(never)]
    fn capture(&self, e: &Entry<V>) {
        let parity = self.parity.load(Ordering::Acquire);
        let st = e.state.load(Ordering::Relaxed);
        if st & STATE_SNAP == parity {
            return;
        }
        if st & STATE_OCCUPIED != 0 {
            let guard = self.sink.read().unwrap_or_else(|p| p.into_inner());
            if let Some(sink) = guard.as_ref() {
                sink(e.key.as_str(), unsafe { (*e.value.get()).assume_init_ref() });
            }
        }
        e.state.store((st & !STATE_SNAP) | parity, Ordering::Relaxed);
    }

    pub(crate) fn begin(&self, sink: SnapSink<V>) -> bool {
        if self.active.load(Ordering::Acquire) {
            return false;
        }
        *self.sink.write().unwrap_or_else(|p| p.into_inner()) = Some(sink);
        self.parity.fetch_xor(STATE_SNAP, Ordering::AcqRel);
        self.active.store(true, Ordering::Release);
        true
    }

    pub(crate) fn end(&self) {
        self.active.store(false, Ordering::Release);
        *self.sink.write().unwrap_or_else(|p| p.into_inner()) = None;
    }
}
