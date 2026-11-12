use customhash::CustomMap;
use std::sync::{Arc, mpsc};
use std::time::Duration;

#[test]
fn borrowed_values_hold_off_overwrites_until_the_last_reader_finishes() {
   let map = Arc::new(CustomMap::with_capacity(2, 100));
   map.insert_str("key", vec![b'a'; 4096]);
   let first = map.get_ref("key").unwrap();
   let second = map.get_ref("key").unwrap();
   let (started, ready) = mpsc::channel();
   let (finished, done) = mpsc::channel();
   let writer_map = Arc::clone(&map);
   let writer = std::thread::spawn(move || {
      started.send(()).unwrap();
      writer_map.insert_str("key", vec![b'b'; 4096]);
      finished.send(()).unwrap();
   });
   ready.recv_timeout(Duration::from_secs(2)).unwrap();
   let early = done.recv_timeout(Duration::from_millis(50));
   drop(first);
   let after_first = done.recv_timeout(Duration::from_millis(50));
   drop(second);
   writer.join().unwrap();
   assert!(early.is_err(), "writer replaced a borrowed value");
   assert!(after_first.is_err(), "writer bypassed the remaining reader");
   assert_eq!(map.get("key"), Some(vec![b'b'; 4096]));
}

#[test]
fn panicking_read_callback_releases_protection() {
   let map = CustomMap::with_capacity(2, 100);
   map.insert_str("key", String::from("original"));
   let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
      map.get_locked("key", |_| panic!("deliberate reader panic"));
   }));
   assert!(panic.is_err());
   map.insert_str("key", String::from("updated"));
   assert_eq!(map.get("key").as_deref(), Some("updated"));
}

#[test]
fn deleting_and_reinserting_never_leaves_duplicate_live_keys() {
   let map = CustomMap::with_capacity(2, 100);
   for value in 0..1000 {
      map.insert_str("key", value);
      map.set_str("key", value + 1);
      assert_eq!(map.get("key"), Some(value + 1));
      assert_eq!(map.len(), 1);
      assert!(map.remove_no_clone("key"));
      assert_eq!(map.get("key"), None);
      assert_eq!(map.len(), 0);
   }
}

#[test]
fn concurrent_insertions_and_resizes_preserve_every_key_once() {
   let map = Arc::new(CustomMap::with_capacity(1, 8));
   let start = Arc::new(std::sync::Barrier::new(8));
   std::thread::scope(|scope| {
      for worker in 0..8 {
         let map = Arc::clone(&map);
         let start = Arc::clone(&start);
         scope.spawn(move || {
            start.wait();
            for i in 0..2000 {
               let key = format!("worker:{worker}:{i}");
               map.insert_str(&key, (worker, i));
               assert_eq!(map.get(&key), Some((worker, i)));
               map.insert_str("shared", (worker, i));
            }
         });
      }
   });
   assert_eq!(map.len(), 16001);
   let mut seen = std::collections::HashSet::new();
   map.for_each(|key, _| assert!(seen.insert(key.to_owned()), "duplicate key: {key}"));
   assert_eq!(seen.len(), 16001);
   for worker in 0..8 {
      for i in 0..2000 {
         assert_eq!(map.get(&format!("worker:{worker}:{i}")), Some((worker, i)));
      }
   }
}

#[test]
fn conditional_expiration_preserves_a_replaced_value() {
   let map = CustomMap::with_capacity(1, 8);
   map.insert_str("key", 1);
   let observed = map.get("key").unwrap();
   map.insert_str("key", 2);
   assert!(!map.remove_if("key", |value| *value == observed));
   assert_eq!(map.get("key"), Some(2));
   assert!(map.remove_if("key", |value| *value == 2));
   assert_eq!(map.len(), 0);
}

#[test]
fn failed_insertion_claims_drop_their_unused_values() {
   #[derive(Clone)]
   struct Value(Arc<std::sync::atomic::AtomicUsize>);
   impl Drop for Value {
      fn drop(&mut self) {
         self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
      }
   }
   let map = Arc::new(CustomMap::with_capacity(1, 100));
   let barrier = Arc::new(std::sync::Barrier::new(8));
   let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
   std::thread::scope(|scope| {
      for _ in 0..8 {
         let map = Arc::clone(&map);
         let barrier = Arc::clone(&barrier);
         let drops = Arc::clone(&drops);
         scope.spawn(move || {
            let _guard = customhash::pin();
            let (hash, _) = map.locate_key("shared");
            map.probe_or_insert_fast(hash, "shared", || {
               barrier.wait();
               Value(drops)
            });
         });
      }
   });
   assert_eq!(map.len(), 1);
   assert_eq!(drops.load(std::sync::atomic::Ordering::Relaxed), 7);
}

#[test]
fn protected_read_batches_hold_off_every_mutation_path() {
   for case in 0..19 {
      let map = Arc::new(CustomMap::with_capacity(1, 16));
      map.insert_str("key", 1u64);
      let reader = map.try_read_epoch().unwrap();
      assert!(map.read_epoch_active());
      assert!(map.try_read_epoch().is_none());
      let writer_map = Arc::clone(&map);
      let (started, ready) = mpsc::channel();
      let (finished, done) = mpsc::channel();
      let writer = std::thread::spawn(move || {
         let _pin = customhash::pin();
         let (hash, shard) = writer_map.locate_key("key");
         let entry = writer_map.probe_slot_entry(hash, shard).cast();
         started.send(()).unwrap();
         match case {
            0 => {
               writer_map.insert_str("key", 2);
            }
            1 => {
               writer_map.insert(String::from("key"), 2);
            }
            2 => {
               writer_map.set_str("key", 2);
            }
            3 => {
               writer_map.remove_no_clone("key");
            }
            4 => {
               writer_map.update_with("key", |v| *v += 1);
            }
            5 => {
               writer_map.update("key", |v| (*v + 1, ()));
            }
            6 => {
               writer_map.try_update("key", |v| Some((*v + 1, ())));
            }
            7 => {
               writer_map.clear();
            }
            8 => {
               writer_map.retain(|_, _| false);
            }
            9 => {
               writer_map.retain_shard_range(0, 0, usize::MAX, |_, _| false);
            }
            10 => {
               writer_map.defragment_shard_range(0, 0, usize::MAX, |v| *v += 1);
            }
            11 => {
               writer_map.expire_shard_range(0, 0, usize::MAX, |_| Some(true));
            }
            12 => {
               writer_map.remove_with("key", |_| ());
            }
            13 => {
               writer_map.insert_if_absent_str("key", 2);
            }
            14 => {
               writer_map.set_with_hint("key", hash, entry, 2);
            }
            15 => {
               writer_map.update_with_hint("key", hash, entry, |v| *v += 1);
            }
            16 => {
               writer_map.probe_or_insert_fast(hash, "key", || 2);
            }
            17 => {
               writer_map.insert_slot_fast_located(hash, shard, "key", 2);
            }
            18 => {
               writer_map.compact_shard(0);
            }
            _ => unreachable!(),
         }
         finished.send(()).unwrap();
      });
      ready.recv_timeout(Duration::from_secs(2)).unwrap();
      let early = done.recv_timeout(Duration::from_millis(30));
      drop(reader);
      writer.join().unwrap();
      assert!(
         early.is_err(),
         "mutation path {case} bypassed the read batch"
      );
      assert!(!map.read_epoch_active());
      assert!(map.try_read_epoch().is_some());
   }
}

#[test]
fn nested_write_guards_can_be_dropped_out_of_order() {
   let first = CustomMap::<u64>::with_capacity(1, 8);
   let second = CustomMap::<u64>::with_capacity(1, 8);
   let outer = first.write_epoch();
   let inner = first.write_epoch();
   drop(outer);
   assert!(first.try_read_epoch().is_none());
   let other = second.write_epoch();
   drop(inner);
   assert!(first.try_read_epoch().is_some());
   assert!(second.try_read_epoch().is_none());
   drop(other);
   assert!(first.try_read_epoch().is_some());
   assert!(second.try_read_epoch().is_some());
   first.insert_str("key", 1);
   second.insert_str("key", 2);
   let released = std::sync::atomic::AtomicBool::new(false);
   let mut wrote_early = false;
   std::thread::scope(|scope| {
      let (started, ready) = mpsc::channel();
      let (release, wait) = mpsc::channel();
      let reader_map = &first;
      let reader_released = &released;
      scope.spawn(move || {
         let reader = reader_map.try_read_epoch().unwrap();
         started.send(()).unwrap();
         wait.recv().unwrap();
         reader_released.store(true, std::sync::atomic::Ordering::Release);
         drop(reader);
      });
      ready.recv_timeout(Duration::from_secs(2)).unwrap();
      scope.spawn(move || {
         std::thread::sleep(Duration::from_millis(50));
         release.send(()).unwrap();
      });
      first.update_with("key", |value| {
         wrote_early = !released.load(std::sync::atomic::Ordering::Acquire);
         *value += 1;
      });
   });
   assert!(
      !wrote_early,
      "stale writer context bypassed reader protection"
   );
   assert_eq!(first.get("key"), Some(2));
}

#[test]
fn forgotten_guards_cannot_match_a_replacement_map_at_the_same_address() {
   let mut map = Box::new(CustomMap::<u64>::with_capacity(1, 8));
   let guard = map.write_epoch();
   std::mem::forget(guard);
   *map = CustomMap::with_capacity(1, 8);
   let guard = map.write_epoch();
   assert!(map.try_read_epoch().is_none());
   drop(guard);
   map.insert_str("key", 1);
   assert_eq!(map.get("key"), Some(1));
}
