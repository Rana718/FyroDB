use crossbeam_queue::SegQueue;
use foldhash::fast::FixedState;
use mio::Waker;
use parking_lot::Mutex;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

#[path = "signal.rs"]
mod signal;

pub(crate) const PATTERN_FLAG: u64 = 1 << 63;
const FANOUT_LIMIT: usize = 64 * 1024 * 1024;
const SUBSCRIBER_LIMIT: usize = 32 * 1024 * 1024;
const OVERFLOW: usize = 1 << (usize::BITS - 1);

pub struct FanEntry {
   pub channel: Arc<str>,
   pub frame: Arc<[u8]>,
   epoch: u64,
}

impl FanEntry {
   pub(crate) fn new(channel: Arc<str>, frame: Arc<[u8]>, generation: u64, pattern: bool) -> Self {
      assert_eq!(generation & PATTERN_FLAG, 0);
      Self {
         channel,
         frame,
         epoch: generation | if pattern { PATTERN_FLAG } else { 0 },
      }
   }
   pub(crate) fn generation(&self) -> u64 {
      self.epoch & !PATTERN_FLAG
   }
   pub(crate) fn is_pattern(&self) -> bool {
      self.epoch & PATTERN_FLAG != 0
   }
   pub(crate) fn same_subscription(&self, other: &Self) -> bool {
      self.epoch == other.epoch && self.channel == other.channel
   }
   fn charge(&self) -> usize {
      self
         .frame
         .len()
         .saturating_add(std::mem::size_of::<Self>() + std::mem::size_of::<usize>())
   }
}

pub enum Completion {
   Reply {
      token: usize,
      uid: u64,
      bytes: Vec<u8>,
   },
   Retry {
      token: usize,
      uid: u64,
   },
}

#[derive(Clone, Copy)]
pub(crate) struct Subscriber {
   pub token: usize,
   pub generation: u64,
}

type Members = SmallVec<[Subscriber; 1]>;
type Subscriptions = HashMap<Arc<str>, Members, FixedState>;
pub(crate) struct LocalSubscriptions {
   pub channels: Subscriptions,
   pub patterns: Subscriptions,
}
impl LocalSubscriptions {
   fn new() -> Self {
      Self {
         channels: HashMap::with_hasher(FixedState::with_seed(0)),
         patterns: HashMap::with_hasher(FixedState::with_seed(0)),
      }
   }
}

pub struct WorkerNotifier {
   pub pending: SegQueue<usize>,
   pub(crate) fanout: SegQueue<FanEntry>,
   pub completions: SegQueue<Completion>,
   completion_pending: AtomicBool,
   local_subs: Mutex<Option<Box<LocalSubscriptions>>>,
   pub waker: Arc<Waker>,
   worker_index: usize,
   wake_pending: AtomicBool,
   fanout_pending: AtomicBool,
   fanout_bytes: AtomicUsize,
   fanout_overflow: AtomicBool,
}

impl WorkerNotifier {
   pub fn new(waker: Arc<Waker>, worker_index: usize) -> Arc<Self> {
      Arc::new(Self {
         pending: SegQueue::new(),
         fanout: SegQueue::new(),
         completions: SegQueue::new(),
         completion_pending: AtomicBool::new(false),
         local_subs: Mutex::new(None),
         waker,
         worker_index,
         wake_pending: AtomicBool::new(false),
         fanout_pending: AtomicBool::new(false),
         fanout_bytes: AtomicUsize::new(0),
         fanout_overflow: AtomicBool::new(false),
      })
   }

   #[inline(always)]
   pub fn worker_index(&self) -> usize {
      self.worker_index
   }

   #[inline]
   pub fn notify(&self, token: usize) {
      self.pending.push(token);
      if !signal::arm(&self.wake_pending) {
         let _ = self.waker.wake();
      }
   }

   #[inline]
   pub fn complete(&self, completion: Completion) {
      self.completions.push(completion);
      if !signal::arm(&self.completion_pending) {
         let _ = self.waker.wake();
      }
   }

   pub fn drain_completions(&self, out: &mut Vec<Completion>) {
      loop {
         while let Some(completion) = self.completions.pop() {
            out.push(completion);
         }
         signal::disarm(&self.completion_pending);
         if self.completions.is_empty() || signal::arm(&self.completion_pending) {
            return;
         }
      }
   }

   #[inline]
   pub fn notify_fanout(&self, entry: FanEntry) {
      let charge = entry.charge();
      if self
         .fanout_bytes
         .try_update(Ordering::AcqRel, Ordering::Relaxed, |bytes| {
            bytes.checked_add(charge).filter(|n| *n <= FANOUT_LIMIT)
         })
         .is_err()
      {
         if !self.fanout_overflow.swap(true, Ordering::AcqRel) {
            let _ = self.waker.wake();
         }
         return;
      }
      self.fanout.push(entry);
      if !signal::arm(&self.fanout_pending) {
         let _ = self.waker.wake();
      }
   }

   pub fn drain_fanout(&self, f: impl FnMut(FanEntry)) -> usize {
      self.drain_fanout_limit(usize::MAX, usize::MAX, f)
   }

   pub(crate) fn drain_fanout_limit(
      &self,
      max_entries: usize,
      max_bytes: usize,
      mut f: impl FnMut(FanEntry),
   ) -> usize {
      let mut seen = 0usize;
      let mut bytes = 0usize;
      // Publishers and this drainer RMW the same `fanout_bytes` line; fold
      // the consumer's charges into one release per drain instead of one
      // RMW per entry.
      let mut freed = 0usize;
      macro_rules! settle_return {
         () => {
            if freed != 0 {
               self.fanout_bytes.fetch_sub(freed, Ordering::Release);
            }
            return seen;
         };
      }
      loop {
         while seen < max_entries && bytes < max_bytes {
            let Some(entry) = self.fanout.pop() else {
               break;
            };
            let charge = entry.charge();
            freed += charge;
            seen += 1;
            bytes = bytes.saturating_add(charge);
            f(entry);
         }
         signal::disarm(&self.fanout_pending);
         if self.fanout.is_empty() {
            settle_return!();
         }
         if seen >= max_entries || bytes >= max_bytes {
            if !signal::arm(&self.fanout_pending) {
               let _ = self.waker.wake();
            }
            settle_return!();
         }
         if signal::arm(&self.fanout_pending) {
            settle_return!();
         }
      }
   }

   pub(crate) fn take_fanout_overflow(&self) -> bool {
      self.fanout_overflow.swap(false, Ordering::AcqRel)
   }

   pub fn local_subscribers(&self, channel: &str) -> Option<Vec<usize>> {
      self
         .local_subs
         .lock()
         .as_ref()
         .and_then(|subs| subs.channels.get(channel))
         .map(|members| members.iter().map(|member| member.token).collect())
   }

   pub(crate) fn with_local_subs<R>(&self, f: impl FnOnce(Option<&LocalSubscriptions>) -> R) -> R {
      f(self.local_subs.lock().as_deref())
   }

   pub fn register_local(&self, channel: &str, token: usize) {
      let mut local = self.local_subs.lock();
      let subs = local.get_or_insert_with(|| Box::new(LocalSubscriptions::new()));
      if let Some(members) = subs.channels.get_mut(channel) {
         if !members.iter().any(|m| m.token == token) {
            members.push(Subscriber {
               token,
               generation: 0,
            });
         }
      } else {
         subs.channels.insert(
            Arc::from(channel),
            smallvec::smallvec![Subscriber {
               token,
               generation: 0
            }],
         );
      }
   }

   pub(crate) fn register_subscription(
      &self,
      name: Arc<str>,
      token: usize,
      generation: u64,
      pattern: bool,
   ) {
      let mut local = self.local_subs.lock();
      let subs = local.get_or_insert_with(|| Box::new(LocalSubscriptions::new()));
      let map = if pattern {
         &mut subs.patterns
      } else {
         &mut subs.channels
      };
      let members = map.entry(name).or_default();
      if let Some(member) = members.iter_mut().find(|member| member.token == token) {
         member.generation = generation;
      } else {
         members.push(Subscriber { token, generation });
      }
   }

   pub fn unregister_local(&self, channel: &str, token: usize) {
      self.unregister_subscription(channel, token, false);
   }

   pub(crate) fn unregister_subscription(&self, name: &str, token: usize, pattern: bool) {
      let mut local = self.local_subs.lock();
      let Some(subs) = local.as_deref_mut() else {
         return;
      };
      let map = if pattern {
         &mut subs.patterns
      } else {
         &mut subs.channels
      };
      if let Some(members) = map.get_mut(name) {
         members.retain(|member| member.token != token);
         if members.is_empty() {
            map.remove(name);
         }
      }
      if subs.channels.is_empty() && subs.patterns.is_empty() {
         *local = None;
      }
   }

   #[inline]
   pub fn drain_pending_into(&self, out: &mut Vec<usize>) {
      loop {
         while let Some(token) = self.pending.pop() {
            out.push(token);
         }
         signal::disarm(&self.wake_pending);
         if self.pending.is_empty() || signal::arm(&self.wake_pending) {
            return;
         }
      }
   }
}

pub struct SubSlot {
   pub token: usize,
   queue: SegQueue<Arc<[u8]>>,
   notify_pending: AtomicBool,
   notifier: Arc<WorkerNotifier>,
   queued_bytes: AtomicUsize,
}

impl SubSlot {
   pub fn new(token: usize, notifier: Arc<WorkerNotifier>) -> Self {
      Self {
         token,
         queue: SegQueue::new(),
         notify_pending: AtomicBool::new(false),
         notifier,
         queued_bytes: AtomicUsize::new(0),
      }
   }
   #[inline(always)]
   pub fn notifier(&self) -> &Arc<WorkerNotifier> {
      &self.notifier
   }

   #[inline]
   pub fn push(&self, msg: Arc<[u8]>) {
      self.push_inner(msg, true);
   }
   pub(crate) fn push_local(&self, msg: Arc<[u8]>) {
      self.push_inner(msg, false);
   }
   fn push_inner(&self, msg: Arc<[u8]>, notify: bool) {
      let charge = msg
         .len()
         .saturating_add(std::mem::size_of::<Arc<[u8]>>() + std::mem::size_of::<usize>());
      if self
         .queued_bytes
         .try_update(Ordering::AcqRel, Ordering::Relaxed, |bytes| {
            if bytes & OVERFLOW != 0 {
               None
            } else {
               bytes.checked_add(charge).filter(|n| *n <= SUBSCRIBER_LIMIT)
            }
         })
         .is_err()
      {
         if self.queued_bytes.fetch_or(OVERFLOW, Ordering::AcqRel) & OVERFLOW == 0 && notify {
            self.notifier.notify(self.token);
         }
         return;
      }
      self.queue.push(msg);
      if notify && !signal::arm(&self.notify_pending) {
         self.notifier.notify(self.token);
      }
   }

   #[inline]
   pub fn drain_into(&self, out: &mut Vec<u8>) {
      self.drain_into_limit(out, usize::MAX);
   }

   pub fn drain_into_limit(&self, out: &mut Vec<u8>, max_bytes: usize) {
      signal::disarm(&self.notify_pending);
      while let Some(msg) = self.queue.pop() {
         let charge = msg
            .len()
            .saturating_add(std::mem::size_of::<Arc<[u8]>>() + std::mem::size_of::<usize>());
         self.queued_bytes.fetch_sub(charge, Ordering::Release);
         out.extend_from_slice(&msg);
         if out.len() >= max_bytes {
            break;
         }
      }
      if !self.queue.is_empty() && !signal::arm(&self.notify_pending) {
         self.notifier.notify(self.token);
      }
   }
   #[inline]
   pub fn has_pending(&self) -> bool {
      !self.queue.is_empty()
   }
   #[inline]
   pub fn queue_len(&self) -> usize {
      self.queue.len()
   }
   pub(crate) fn limit_exceeded(&self, buffered: usize) -> bool {
      let (bytes, overflow) = self.backlog_state();
      overflow
         || bytes
            .checked_add(buffered)
            .is_none_or(|total| total > SUBSCRIBER_LIMIT)
   }
   pub(crate) fn backlog_state(&self) -> (usize, bool) {
      let state = self.queued_bytes.load(Ordering::Acquire);
      (state & !OVERFLOW, state & OVERFLOW != 0)
   }
}

impl crate::storage::blocking::Wake for WorkerNotifier {
   fn wake_retry(&self, token: usize, uid: u64) {
      self.complete(Completion::Retry { token, uid });
   }
}

#[cfg(test)]
mod tests {
   use super::*;
   fn notifier() -> Arc<WorkerNotifier> {
      let poll = mio::Poll::new().unwrap();
      WorkerNotifier::new(
         Arc::new(Waker::new(poll.registry(), mio::Token(1)).unwrap()),
         0,
      )
   }

   #[test]
   fn subscriber_backlogs_are_bounded_in_bytes_and_release_references() {
      let slot = SubSlot::new(1, notifier());
      let payload: Arc<[u8]> = vec![255; 10 * 1024].into();
      for _ in 0..4096 {
         slot.push(Arc::clone(&payload));
      }
      let (bytes, overflow) = slot.backlog_state();
      assert!(overflow);
      assert!(bytes <= SUBSCRIBER_LIMIT);
      assert!(slot.limit_exceeded(0));
      let mut out = Vec::new();
      while slot.has_pending() {
         out.clear();
         slot.drain_into_limit(&mut out, payload.len());
         assert_eq!(out, payload.as_ref());
      }
      assert_eq!(slot.backlog_state(), (0, true));
      assert_eq!(Arc::strong_count(&payload), 1);
   }

   #[test]
   fn fanout_budget_and_drain_limits_keep_queue_accounting_consistent() {
      let notifier = notifier();
      let payload: Arc<[u8]> = vec![0; 64 * 1024].into();
      for _ in 0..2048 {
         notifier.notify_fanout(FanEntry::new(
            Arc::from("channel"),
            Arc::clone(&payload),
            1,
            false,
         ));
      }
      assert!(notifier.fanout_bytes.load(Ordering::Acquire) <= FANOUT_LIMIT);
      assert!(notifier.take_fanout_overflow());
      assert!(!notifier.take_fanout_overflow());
      let mut seen = 0;
      let drained = notifier.drain_fanout_limit(8, 256 * 1024, |_| seen += 1);
      assert_eq!(seen, drained);
      assert!(drained <= 8);
      assert!(!notifier.fanout.is_empty());
      notifier.drain_fanout(|_| {});
      assert_eq!(notifier.fanout_bytes.load(Ordering::Acquire), 0);
      assert_eq!(Arc::strong_count(&payload), 1);
   }
}
