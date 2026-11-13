use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[path = "../src/pubsub/signal.rs"]
mod signal;

#[test]
fn rearming_cannot_strand_a_publication_or_completion_without_a_wake() {
   loom::model(|| {
      let pending = Arc::new(AtomicBool::new(true));
      let queue = Arc::new(AtomicBool::new(false));
      let wake = Arc::new(AtomicBool::new(false));
      let p = pending.clone();
      let q = queue.clone();
      let w = wake.clone();
      let producer = loom::thread::spawn(move || {
         q.store(true, Ordering::SeqCst);
         if !signal::arm(&p) {
            w.store(true, Ordering::Release);
         }
      });
      signal::disarm(&pending);
      let seen = queue.load(Ordering::SeqCst);
      producer.join().unwrap();
      assert!(
         seen || wake.load(Ordering::Acquire),
         "item stranded without a wake"
      );
   });
}

#[test]
fn concurrent_producers_leave_a_wake_for_every_unseen_item() {
   loom::model(|| {
      let pending = Arc::new(AtomicBool::new(true));
      let queue = Arc::new(AtomicUsize::new(0));
      let wake = Arc::new(AtomicBool::new(false));
      let producers: Vec<_> = (0..2)
         .map(|i| {
            let p = pending.clone();
            let q = queue.clone();
            let w = wake.clone();
            loom::thread::spawn(move || {
               q.fetch_or(1 << i, Ordering::SeqCst);
               if !signal::arm(&p) {
                  w.store(true, Ordering::Release);
               }
            })
         })
         .collect();
      signal::disarm(&pending);
      let seen = queue.load(Ordering::SeqCst);
      for producer in producers {
         producer.join().unwrap();
      }
      assert!(
         seen == 3 || wake.load(Ordering::Acquire),
         "unseen items stranded without a wake"
      );
   });
}
