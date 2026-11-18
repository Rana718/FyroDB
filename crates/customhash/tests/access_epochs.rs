use customhash::CustomMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, mpsc};
use std::time::Duration;

#[test]
fn read_epochs_keep_readers_and_writers_disjoint_under_contention() {
   let map = Arc::new(CustomMap::<u64>::with_capacity(1, 8));
   let start = Arc::new(Barrier::new(17));
   let readers = AtomicUsize::new(0);
   let writers = AtomicUsize::new(0);
   std::thread::scope(|scope| {
      for worker in 0..16 {
         let map = Arc::clone(&map);
         let start = Arc::clone(&start);
         let readers = &readers;
         let writers = &writers;
         scope.spawn(move || {
            start.wait();
            let mut completed = 0;
            while completed < 1000 {
               if worker % 2 == 0 {
                  let Some(guard) = map.try_read_epoch() else {
                     std::thread::yield_now();
                     continue;
                  };
                  readers.fetch_add(1, Ordering::SeqCst);
                  assert_eq!(writers.load(Ordering::SeqCst), 0);
                  if completed % 16 == 0 {
                     std::thread::yield_now();
                  }
                  assert_eq!(writers.load(Ordering::SeqCst), 0);
                  readers.fetch_sub(1, Ordering::SeqCst);
                  drop(guard);
               } else {
                  let guard = map.write_epoch();
                  writers.fetch_add(1, Ordering::SeqCst);
                  assert_eq!(readers.load(Ordering::SeqCst), 0);
                  if completed % 16 == 0 {
                     std::thread::yield_now();
                  }
                  assert_eq!(readers.load(Ordering::SeqCst), 0);
                  writers.fetch_sub(1, Ordering::SeqCst);
                  drop(guard);
               }
               completed += 1;
            }
         });
      }
      start.wait();
   });
   assert_eq!(readers.load(Ordering::Relaxed), 0);
   assert_eq!(writers.load(Ordering::Relaxed), 0);
   assert!(map.try_read_epoch().is_some());
}

#[test]
fn a_writer_waits_for_every_concurrent_read_epoch() {
   let map = Arc::new(CustomMap::<u64>::with_capacity(1, 8));
   let ready = Arc::new(Barrier::new(18));
   let mut release = Vec::new();
   let (done, finished) = mpsc::channel();
   let (wrote, written) = mpsc::channel();
   let mut early = false;
   std::thread::scope(|scope| {
      for _ in 0..17 {
         let map = Arc::clone(&map);
         let ready = Arc::clone(&ready);
         let (send, receive) = mpsc::channel();
         release.push(send);
         let done = done.clone();
         scope.spawn(move || {
            let guard = map.try_read_epoch().unwrap();
            ready.wait();
            receive.recv().unwrap();
            drop(guard);
            done.send(()).unwrap();
         });
      }
      ready.wait();
      let map = Arc::clone(&map);
      scope.spawn(move || {
         let _guard = map.write_epoch();
         wrote.send(()).unwrap();
      });
      early |= written.recv_timeout(Duration::from_millis(20)).is_ok();
      for send in release.iter().take(16) {
         send.send(()).unwrap();
         finished.recv_timeout(Duration::from_secs(2)).unwrap();
      }
      early |= written.recv_timeout(Duration::from_millis(20)).is_ok();
      release[16].send(()).unwrap();
   });
   assert!(
      !early,
      "writer bypassed a remaining reader while a guard remains active"
   );
   assert!(written.recv_timeout(Duration::from_secs(2)).is_ok());
   assert!(map.try_read_epoch().is_some());
}

#[test]
fn all_write_epochs_block_read_admission_until_the_last_writer_exits() {
   let map = Arc::new(CustomMap::<u64>::with_capacity(1, 8));
   let ready = Arc::new(Barrier::new(18));
   let mut release = Vec::new();
   let (done, finished) = mpsc::channel();
   let mut early = false;
   std::thread::scope(|scope| {
      for _ in 0..17 {
         let map = Arc::clone(&map);
         let ready = Arc::clone(&ready);
         let (send, receive) = mpsc::channel();
         release.push(send);
         let done = done.clone();
         scope.spawn(move || {
            let guard = map.write_epoch();
            ready.wait();
            receive.recv().unwrap();
            drop(guard);
            done.send(()).unwrap();
         });
      }
      ready.wait();
      early |= map.try_read_epoch().is_some();
      for send in &release {
         early |= map.try_read_epoch().is_some();
         send.send(()).unwrap();
         finished.recv_timeout(Duration::from_secs(2)).unwrap();
      }
   });
   assert!(
      !early,
      "reader bypassed a remaining writer while a guard remains active"
   );
   assert!(map.try_read_epoch().is_some());
}

#[test]
fn nested_maps_preserve_out_of_order_guard_drops_and_unwinding() {
   let first = CustomMap::<u64>::with_capacity(1, 8);
   let second = CustomMap::<u64>::with_capacity(1, 8);
   let third = CustomMap::<u64>::with_capacity(1, 8);
   let a = first.write_epoch();
   let b = second.write_epoch();
   let c = first.write_epoch();
   let d = third.write_epoch();
   drop(a);
   drop(b);
   assert!(first.try_read_epoch().is_none());
   assert!(second.try_read_epoch().is_some());
   assert!(third.try_read_epoch().is_none());
   drop(c);
   drop(d);
   let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
      let _guard = first.write_epoch();
      panic!("deliberate guarded panic");
   }));
   assert!(caught.is_err());
   assert!(first.try_read_epoch().is_some());
   assert!(second.try_read_epoch().is_some());
   assert!(third.try_read_epoch().is_some());
   first.insert_str("key", 7);
   assert_eq!(first.get("key"), Some(7));
}

#[test]
fn snapshot_waits_for_an_admitted_writer_and_holds_both_keys_stable() {
   let map = CustomMap::<usize>::with_capacity(2, 8);
   let (started, starting) = mpsc::channel();
   let (sampled, sample) = mpsc::channel();
   std::thread::scope(|scope| {
      let writer = map.write_epoch();
      map.insert_str("a", 1);
      scope.spawn(|| {
         started.send(()).unwrap();
         let snapshot = map.read_epoch();
         let values = (map.get("a"), map.get("b"));
         drop(snapshot);
         sampled.send(values).unwrap();
      });
      starting.recv_timeout(Duration::from_secs(2)).unwrap();
      assert!(matches!(
         sample.recv_timeout(Duration::from_millis(20)),
         Err(mpsc::RecvTimeoutError::Timeout)
      ));
      map.insert_str("b", 2);
      drop(writer);
      assert_eq!(
         sample.recv_timeout(Duration::from_secs(2)).unwrap(),
         (Some(1), Some(2))
      );
   });
}

#[test]
fn deeply_nested_writes_release_every_map_out_of_order() {
   let maps: Vec<_> = (0..12).map(|_| CustomMap::<usize>::new()).collect();
   let guards: Vec<_> = maps.iter().map(CustomMap::write_epoch).collect();
   let nested = maps[0].write_epoch();
   for (i, guard) in guards.into_iter().enumerate() {
      assert!(maps[i].try_read_epoch().is_none());
      drop(guard);
      assert_eq!(maps[i].try_read_epoch().is_some(), i != 0);
   }
   drop(nested);
   assert!(maps[0].try_read_epoch().is_some());
}

#[test]
fn point_read_stamps_detect_writers_and_map_changes() {
   let map = CustomMap::<usize>::new();
   let other = CustomMap::<usize>::new();
   let stamp = map.read_stamp().unwrap();
   assert!(!other.stamp_is_current(&stamp));
   let writer = map.write_epoch();
   assert!(!map.stamp_is_current(&stamp));
   assert!(map.read_stamp().is_none());
   map.insert_str("hot", 1);
   drop(writer);
   let updated = map.read_stamp().unwrap();
   assert!(map.stamp_is_current(&updated));
   assert!(!map.stamp_is_current(&stamp));
   map.clear();
   assert!(!map.stamp_is_current(&updated));
}
