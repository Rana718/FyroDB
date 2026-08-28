use crossbeam_queue::SegQueue;
use mio::Waker;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct WorkerNotifier {
    pub pending: SegQueue<usize>,
    pub waker: Arc<Waker>,
    wake_pending: AtomicBool,
}

impl WorkerNotifier {
    pub fn new(waker: Arc<Waker>) -> Arc<Self> {
        Arc::new(Self {
            pending: SegQueue::new(),
            waker,
            wake_pending: AtomicBool::new(false),
        })
    }

    #[inline]
    pub fn notify(&self, token: usize) {
        self.pending.push(token);
        if !self.wake_pending.swap(true, Ordering::AcqRel) {
            let _ = self.waker.wake();
        }
    }

    #[inline]
    pub fn drain_pending_into(&self, out: &mut Vec<usize>) {
        loop {
            while let Some(token) = self.pending.pop() {
                out.push(token);
            }

            self.wake_pending.store(false, Ordering::Release);
            if self.pending.is_empty() {
                return;
            }

            if !self.wake_pending.swap(true, Ordering::AcqRel) {
                continue;
            }
            return;
        }
    }
}

pub struct SubSlot {
    pub token: usize,
    pub queue: SegQueue<Arc<[u8]>>,
    notify_pending: AtomicBool,
    notifier: Arc<WorkerNotifier>,
}

impl SubSlot {
    pub fn new(token: usize, notifier: Arc<WorkerNotifier>) -> Self {
        Self {
            token,
            queue: SegQueue::new(),
            notify_pending: AtomicBool::new(false),
            notifier,
        }
    }

    /// Enqueue one frame for this subscriber.
    ///
    /// The queue tracks its own length, so no separate counter is maintained.
    #[inline]
    pub fn push(&self, msg: Arc<[u8]>) {
        self.queue.push(msg);
        if !self.notify_pending.swap(true, Ordering::AcqRel) {
            self.notifier.notify(self.token);
        }
    }

    #[inline]
    pub fn drain_into(&self, out: &mut Vec<u8>) {
        self.drain_into_limit(out, usize::MAX);
    }

    pub fn drain_into_limit(&self, out: &mut Vec<u8>, max_bytes: usize) {
        self.notify_pending.store(false, Ordering::Release);
        while let Some(msg) = self.queue.pop() {
            out.extend_from_slice(&msg);
            if out.len() >= max_bytes {
                break;
            }
        }
        if !self.queue.is_empty() && !self.notify_pending.swap(true, Ordering::AcqRel) {
            self.notifier.notify(self.token);
        }
    }

    #[inline]
    pub fn has_pending(&self) -> bool {
        !self.queue.is_empty()
    }

    /// Backlog size, used to shed slow subscribers.
    ///
    /// Derived from the queue's internal head/tail indices — no maintained
    /// counter needed.
    #[inline]
    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }
}
