use crossbeam_queue::SegQueue;
use mio::Waker;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct FanEntry {
   pub channel: Arc<str>,
   pub frame: Arc<[u8]>,
}

/// Work finished elsewhere for a connection parked on this worker.
pub enum Completion {
   /// Reply bytes for the off-worker command the connection waits on.
   Reply {
      token: usize,
      uid: u64,
      bytes: Vec<u8>,
   },
   /// A key the connection's blocking command waits on was written.
   Retry { token: usize, uid: u64 },
}

pub struct WorkerNotifier {
   pub pending: SegQueue<usize>,
   pub fanout: SegQueue<FanEntry>,
   pub completions: SegQueue<Completion>,
   completion_pending: AtomicBool,
   local_subs: Mutex<HashMap<String, Vec<usize>>>,
   pub waker: Arc<Waker>,
   worker_index: usize,
   wake_pending: AtomicBool,
   fanout_pending: AtomicBool,
}

impl WorkerNotifier {
   pub fn new(waker: Arc<Waker>, worker_index: usize) -> Arc<Self> {
      Arc::new(Self {
         pending: SegQueue::new(),
         fanout: SegQueue::new(),
         completions: SegQueue::new(),
         completion_pending: AtomicBool::new(false),
         local_subs: Mutex::new(HashMap::new()),
         waker,
         worker_index,
         wake_pending: AtomicBool::new(false),
         fanout_pending: AtomicBool::new(false),
      })
   }

   /// Stable identity of the worker this notifier belongs to. The publish
   /// path uses it to group subscribers without pointer comparisons.
   #[inline(always)]
   pub fn worker_index(&self) -> usize {
      self.worker_index
   }

   #[inline]
   pub fn notify(&self, token: usize) {
      self.pending.push(token);
      if !self.wake_pending.swap(true, Ordering::AcqRel) {
         let _ = self.waker.wake();
      }
   }

   /// Hand a finished result (or a retry) back to this worker's loop.
   #[inline]
   pub fn complete(&self, c: Completion) {
      self.completions.push(c);
      if !self.completion_pending.swap(true, Ordering::AcqRel) {
         let _ = self.waker.wake();
      }
   }

   /// Pop every queued completion into `out`, using a re-arm protocol so
   /// no wakeups are lost between draining and resetting the flag.
   pub fn drain_completions(&self, out: &mut Vec<Completion>) {
      loop {
         while let Some(c) = self.completions.pop() {
            out.push(c);
         }
         self.completion_pending.store(false, Ordering::Release);
         if self.completions.is_empty() || self.completion_pending.swap(true, Ordering::AcqRel) {
            return;
         }
      }
   }

   /// Route one frame to this worker for local fan-out.
   #[inline]
   pub fn notify_fanout(&self, entry: FanEntry) {
      self.fanout.push(entry);
      if !self.fanout_pending.swap(true, Ordering::AcqRel) {
         let _ = self.waker.wake();
      }
   }

   /// Pop every queued fan-out entry; call `f(entry)` for each. Returns the
   /// number of entries seen.
   pub fn drain_fanout(&self, mut f: impl FnMut(FanEntry)) -> usize {
      let mut seen = 0usize;
      loop {
         while let Some(entry) = self.fanout.pop() {
            seen += 1;
            f(entry);
         }
         self.fanout_pending.store(false, Ordering::Release);
         if self.fanout.is_empty() {
            return seen;
         }
         if !self.fanout_pending.swap(true, Ordering::AcqRel) {
            continue;
         }
         return seen;
      }
   }

   /// Subscribers of `channel` living on this worker, as connection tokens.
   pub fn local_subscribers(&self, channel: &str) -> Option<Vec<usize>> {
      self
         .local_subs
         .lock()
         .unwrap_or_else(|e| e.into_inner())
         .get(channel)
         .cloned()
   }

   pub fn with_local_subs<R>(&self, f: impl FnOnce(&HashMap<String, Vec<usize>>) -> R) -> R {
      f(&self.local_subs.lock().unwrap_or_else(|e| e.into_inner()))
   }

   pub fn register_local(&self, channel: &str, token: usize) {
      let mut subs = self.local_subs.lock().unwrap_or_else(|e| e.into_inner());
      let tokens = subs.entry(channel.to_string()).or_default();
      if !tokens.contains(&token) {
         tokens.push(token);
      }
   }

   pub fn unregister_local(&self, channel: &str, token: usize) {
      let mut subs = self.local_subs.lock().unwrap_or_else(|e| e.into_inner());
      if let Some(tokens) = subs.get_mut(channel) {
         tokens.retain(|&t| t != token);
         if tokens.is_empty() {
            subs.remove(channel);
         }
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

   /// The worker this subscriber's connection lives on. The publish path
   /// groups subscribers by this to fan out once per worker.
   #[inline(always)]
   pub fn notifier(&self) -> &Arc<WorkerNotifier> {
      &self.notifier
   }

   /// Enqueue one frame for this subscriber.
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
   #[inline]
   pub fn queue_len(&self) -> usize {
      self.queue.len()
   }
}

impl crate::storage::blocking::Wake for WorkerNotifier {
   fn wake_retry(&self, token: usize, uid: u64) {
      self.complete(Completion::Retry { token, uid });
   }
}
