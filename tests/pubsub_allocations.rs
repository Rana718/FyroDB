use fyro_db::pubsub::{PubSub, SubSlot, WorkerNotifier, frame};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

struct Counted;
thread_local! {
   static ACTIVE: Cell<bool> = const { Cell::new(false) };
   static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}
fn record() {
   if ACTIVE.try_with(Cell::get).unwrap_or(false) {
      let _ = ALLOCATIONS.try_with(|count| count.set(count.get().saturating_add(1)));
   }
}
unsafe impl GlobalAlloc for Counted {
   unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
      let ptr = unsafe { System.alloc(layout) };
      if !ptr.is_null() {
         record();
      }
      ptr
   }
   unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
      let ptr = unsafe { System.alloc_zeroed(layout) };
      if !ptr.is_null() {
         record();
      }
      ptr
   }
   unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
      unsafe { System.dealloc(ptr, layout) };
   }
   unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, bytes: usize) -> *mut u8 {
      let result = unsafe { System.realloc(ptr, layout, bytes) };
      if !result.is_null() {
         record();
      }
      result
   }
}
#[global_allocator]
static ALLOCATOR: Counted = Counted;
struct Stop;
impl Drop for Stop {
   fn drop(&mut self) {
      ACTIVE.with(|active| active.set(false));
   }
}
fn measured<R>(f: impl FnOnce() -> R) -> (R, usize) {
   ALLOCATIONS.with(|count| count.set(0));
   ACTIVE.with(|active| active.set(true));
   let stop = Stop;
   let result = f();
   drop(stop);
   (result, ALLOCATIONS.with(Cell::get))
}
fn notifier() -> Arc<WorkerNotifier> {
   let poll = mio::Poll::new().unwrap();
   WorkerNotifier::new(
      Arc::new(mio::Waker::new(poll.registry(), mio::Token(1)).unwrap()),
      0,
   )
}

#[test]
fn message_and_pattern_frames_allocate_only_the_final_arc() {
   let payload = "p".repeat(10 * 1024);
   for message in ["hello", payload.as_str()] {
      let (_, count) = measured(|| frame::encode_message("channel", message));
      assert_eq!(count, 1);
      let (_, count) = measured(|| frame::encode_pmessage("channel*", "channel", message));
      assert_eq!(count, 1);
   }
}

#[test]
fn variable_batches_have_no_temporary_payload_allocation() {
   let (_, count) = measured(|| frame::encode_batch("variable", &["", "small", "longer-message"]));
   assert_eq!(count, 1);
   let first = frame::encode_batch("cached", &["repeat"; 64]);
   let (second, count) = measured(|| frame::encode_batch("cached", &["repeat"; 64]));
   assert_eq!(count, 0);
   assert!(Arc::ptr_eq(&first, &second));
}

#[test]
fn missing_audiences_do_not_allocate_a_frame() {
   let pubsub = PubSub::new();
   pubsub.publish("warm-up", "warm");
   let payload = "p".repeat(10 * 1024);
   let (_, count) =
      measured(|| assert_eq!(pubsub.publish_batch("nobody", &[payload.as_str(); 512]), 0));
   assert_eq!(count, 0);
}

#[test]
fn snapshot_updates_share_unchanged_subscription_lists() {
   let pubsub = PubSub::new();
   let notifier = notifier();
   let slot = Arc::new(SubSlot::new(1, notifier));
   for i in 0..1024 {
      pubsub.subscribe(&format!("existing:{i}"), Arc::clone(&slot));
   }
   let names: Vec<_> = (0..128).map(|i| format!("new:{i}")).collect();
   let (_, count) = measured(|| {
      for name in &names {
         pubsub.subscribe(name, Arc::clone(&slot));
      }
   });
   assert!(
      count <= names.len() * 5,
      "{count} allocations for {} new channels",
      names.len()
   );
   assert_eq!(pubsub.active_channels(None).len(), 1152);
}

#[test]
fn matching_pattern_frames_are_shared_across_subscribers() {
   let pubsub = PubSub::new();
   let notifier = notifier();
   for token in 0..8 {
      pubsub.psubscribe(
         "topic:*",
         Arc::new(SubSlot::new(token, Arc::clone(&notifier))),
      );
   }
   pubsub.publish("warm-up", "warm");
   pubsub.publish_batch("topic:data", &["prime"; 64]);
   notifier.drain_fanout(|_| {});
   let (_, count) =
      measured(|| assert_eq!(pubsub.publish_batch("topic:data", &["changed"; 64]), 8));
   assert_eq!(count, 1);
   let mut frames = 0;
   notifier.drain_fanout(|_| frames += 1);
   assert_eq!(frames, 1);
}
