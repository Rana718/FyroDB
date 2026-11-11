use fyro_db::storage::{rdb, store::Store};
use std::sync::{Arc, mpsc};
use std::time::Duration;

#[test]
fn zero_interval_disables_background_snapshots() {
   let path = std::env::temp_dir().join(format!(
      "fyrodb-autosave-disabled-{}.rdb",
      std::process::id()
   ));
   let output = path.clone();
   let (done, result) = mpsc::channel();
   let worker = std::thread::spawn(move || {
      let store = Store::with_config(2, usize::MAX);
      rdb::background_save_loop(&store, output.to_str().unwrap(), Duration::ZERO);
      done.send(()).unwrap();
   });
   result.recv_timeout(Duration::from_secs(2)).unwrap();
   worker.join().unwrap();
   assert!(!path.exists());
}

#[test]
fn snapshot_starts_after_an_atomic_transfer_finishes() {
   let store = Arc::new(Store::with_config_workers(2, usize::MAX, 2));
   store.set_string("a", "1000", 0);
   store.set_string("b", "1000", 0);
   let (debited, ready) = mpsc::channel();
   let (finish, proceed) = mpsc::channel();
   let writer_store = Arc::clone(&store);
   let writer = std::thread::spawn(move || {
      let _guard = writer_store.atomic_command_guard();
      writer_store.set_string("a", "999", 0);
      debited.send(()).unwrap();
      proceed.recv().unwrap();
      writer_store.set_string("b", "1001", 0);
   });
   ready.recv_timeout(Duration::from_secs(5)).unwrap();
   let path = std::env::temp_dir().join(format!("fyrodb-isolation-{}.rdb", std::process::id()));
   let snapshot_path = path.clone();
   let (started, waiting) = mpsc::channel();
   let (saved, complete) = mpsc::channel();
   let saver = std::thread::spawn(move || {
      started.send(()).unwrap();
      let result = rdb::save(&store, snapshot_path.to_str().unwrap());
      saved.send(result).unwrap();
   });
   waiting.recv_timeout(Duration::from_secs(5)).unwrap();
   let early = complete.recv_timeout(Duration::from_millis(100));
   finish.send(()).unwrap();
   writer.join().unwrap();
   assert!(
      matches!(early, Err(mpsc::RecvTimeoutError::Timeout)),
      "snapshot observed an unfinished transfer"
   );
   complete
      .recv_timeout(Duration::from_secs(5))
      .unwrap()
      .unwrap();
   saver.join().unwrap();
   let restored = Store::with_config(2, usize::MAX);
   rdb::load(&restored, path.to_str().unwrap()).unwrap();
   std::fs::remove_file(path).unwrap();
   assert_eq!(restored.get("a"), Some("999".to_owned()));
   assert_eq!(restored.get("b"), Some("1001".to_owned()));
}
