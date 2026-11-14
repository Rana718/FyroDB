// Temporary repro: concurrent INCR lost updates at the lib level.
use fyro_db::storage::store::Store;
use std::sync::{Arc, Barrier};

#[test]
fn incr_concurrent_lost_updates_repro() {
    for round in 0..300 {
        let store = Arc::new(Store::with_config_workers(4, usize::MAX, 12));
        store.del("ctr");
        let n = 100;
        let barrier = Arc::new(Barrier::new(2));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let s = Arc::clone(&store);
            let b = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                b.wait();
                for _ in 0..n {
                    s.incr_bytes(b"ctr").unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let final_val = store.get("ctr").unwrap();
        if final_val != "200" {
            panic!("round {round}: lost updates! final={final_val}");
        }
    }
}
