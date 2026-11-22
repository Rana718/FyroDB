// Reusable session threads (a prethreaded server, UNP vol. 1 §30.12).
//
// One thread per client stays the concurrency model, but a finished session's
// thread parks and serves the next connection instead of exiting. Creating a
// thread per connection was not the expensive part; what it implied was: a
// fresh stack mapping and a fresh allocator heap that fetched its buffers from
// the OS and handed them back at exit. That cost ~525 µs of kernel time per
// connection. A reused thread keeps both warm.

use parking_lot::Mutex;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

type Job = Box<dyn FnOnce() + Send + 'static>;

/// Parked threads kept for reuse; extra ones exit after their session.
const MAX_IDLE: usize = 64;
/// A parked thread exits after this long without work.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// How often a parked thread re-checks its idle deadline.
const POLL: Duration = Duration::from_millis(500);

struct Pool {
   /// Rendezvous channel: a send succeeds only if a parked thread is
   /// receiving right now, so a handed-off job can never sit unserved.
   tx: SyncSender<Job>,
   rx: Mutex<Receiver<Job>>,
   idle: AtomicUsize,
   /// Threads created so far (for tests and diagnostics).
   spawned: AtomicUsize,
   name: &'static str,
   stack: usize,
}

impl Pool {
   fn new(name: &'static str, stack: usize) -> Arc<Self> {
      let (tx, rx) = sync_channel::<Job>(0);
      Arc::new(Self {
         tx,
         rx: Mutex::new(rx),
         idle: AtomicUsize::new(0),
         spawned: AtomicUsize::new(0),
         name,
         stack,
      })
   }

   fn spawn(self: &Arc<Self>, job: Job) -> io::Result<()> {
      let job = match self.tx.try_send(job) {
         Ok(()) => return Ok(()),
         Err(TrySendError::Full(job)) | Err(TrySendError::Disconnected(job)) => job,
      };
      self.spawned.fetch_add(1, Ordering::Relaxed);
      let pool = Arc::clone(self);
      thread::Builder::new()
         .name(self.name.into())
         .stack_size(self.stack)
         .spawn(move || {
            job();
            pool.park_and_serve();
         })
         .map(|_| ())
   }

   fn park_and_serve(&self) {
      loop {
         // Bounded pool: past MAX_IDLE parked threads, a finished one exits.
         if self.idle.fetch_add(1, Ordering::AcqRel) >= MAX_IDLE {
            self.idle.fetch_sub(1, Ordering::AcqRel);
            return;
         }
         let job = self.wait_for_job();
         self.idle.fetch_sub(1, Ordering::AcqRel);
         match job {
            Some(job) => job(),
            None => return,
         }
      }
   }

   /// Waits up to `IDLE_TIMEOUT` for a job. Only one parked thread receives at
   /// a time (it holds the lock); the rest wait for the lock with the same
   /// deadline, so idle threads retire on their own schedule.
   fn wait_for_job(&self) -> Option<Job> {
      let deadline = Instant::now() + IDLE_TIMEOUT;
      loop {
         let left = deadline.saturating_duration_since(Instant::now());
         if left.is_zero() {
            return None;
         }
         let Some(guard) = self.rx.try_lock_for(POLL.min(left)) else {
            continue;
         };
         let left = deadline.saturating_duration_since(Instant::now());
         match guard.recv_timeout(POLL.min(left)) {
            Ok(job) => return Some(job),
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => return None,
         }
      }
   }
}

/// Runs `job` on a parked session thread, or on a new one if none is free.
pub(crate) fn spawn_session(
   name: &'static str,
   stack: usize,
   job: impl FnOnce() + Send + 'static,
) -> io::Result<()> {
   static POOL: OnceLock<Arc<Pool>> = OnceLock::new();
   POOL
      .get_or_init(|| Pool::new(name, stack))
      .spawn(Box::new(job))
}

#[cfg(test)]
mod tests {
   use super::*;
   use std::sync::mpsc::channel;

   #[test]
   fn sequential_sessions_reuse_parked_threads() {
      let pool = Pool::new("test-session", 256 * 1024);
      let (tx, rx) = channel();
      for _ in 0..20 {
         let tx = tx.clone();
         pool.spawn(Box::new(move || tx.send(()).unwrap())).unwrap();
         rx.recv().unwrap();
         // Give the finished thread time to park, as between two clients.
         thread::sleep(Duration::from_millis(5));
      }
      let created = pool.spawned.load(Ordering::Relaxed);
      assert!(
         created <= 2,
         "20 sequential sessions created {created} threads"
      );
   }

   #[test]
   fn concurrent_sessions_all_run_and_idle_threads_stay_bounded() {
      let pool = Pool::new("test-session", 256 * 1024);
      let (tx, rx) = channel();
      for i in 0..200 {
         let tx = tx.clone();
         pool
            .spawn(Box::new(move || {
               thread::sleep(Duration::from_millis(1));
               tx.send(i).unwrap();
            }))
            .unwrap();
      }
      drop(tx);
      let mut seen: Vec<i32> = rx.iter().collect();
      seen.sort_unstable();
      assert_eq!(seen, (0..200).collect::<Vec<_>>());
      thread::sleep(Duration::from_millis(50));
      assert!(pool.idle.load(Ordering::Acquire) <= MAX_IDLE);
   }
}
