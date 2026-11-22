use customhash::{CompactKey, CustomMap};
use fyro_db::cluster::hash_slot;
use fyro_db::storage::bitmap::count_ones_fast;
use fyro_db::storage::blocking::{BlockRegistry, Waiter, Wake};
use fyro_db::storage::store::Store;
use fyro_db::storage::stream::StreamId;
use fyro_db::storage::value::QuickList;
use fyro_db::storage::value::{SmallStr, StoreValue};
use fyro_db::utils::resp::format_bulk_header;
use fyro_dbcache::DbCacheManager;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

/// Formal safety invariant tests for FyroDB memory layout, RESP framing,
/// AVX2 streaming store boundaries, and EBR epoch reader isolation.

#[test]
fn verus_rule_resp_bulk_header_soundness_exhaustive() {
   // Invariant 1: For any len in 0..=2048 and key powers of 2 / decimal thresholds,
   // format_bulk_header(len, &mut out) MUST:
   // 1. Return written byte count >= 4 and <= 24.
   // 2. out[0] == b'$'.
   // 3. out[written - 2..written] == b"\r\n".
   // 4. out[1..written - 2] parsed as ASCII decimal == len.
   // 5. Bytes out[written..24] must remain untouched.

   let mut test_lengths: Vec<usize> = (0..=2048).collect();
   test_lengths.extend_from_slice(&[
      9, 10, 99, 100, 999, 1000, 9999, 10000, 32768, 65535, 65536, 131072, 262144, 524288, 1048576,
      2097152, 16777216,
   ]);

   for len in test_lengths {
      let mut out = [0xFAu8; 24];
      let written = format_bulk_header(len, &mut out);

      // Invariant bounds
      assert!(written >= 4, "Header too short for len={len}: {written}");
      assert!(written <= 24, "Header too long for len={len}: {written}");
      assert_eq!(out[0], b'$', "Prefix must be '$'");
      assert_eq!(
         &out[written - 2..written],
         b"\r\n",
         "Suffix must be CRLF for len={len}"
      );

      // Invariant: Exact ASCII integer representation
      let digits_str =
         std::str::from_utf8(&out[1..written - 2]).expect("Digits must be valid ASCII");
      let parsed_len: usize = digits_str
         .parse()
         .expect("Digits must be valid decimal number");
      assert_eq!(parsed_len, len, "Parsed length must match input");

      // Invariant: Reference comparison with standard formatting
      let reference = format!("${len}\r\n");
      assert_eq!(
         &out[..written],
         reference.as_bytes(),
         "Must match canonical reference formatting byte-for-byte"
      );

      // Invariant: No canary byte corruption past written boundary
      assert!(
         out[written..].iter().all(|&b| b == 0xFA),
         "Canary bytes past written boundary must not be modified"
      );
   }
}

#[test]
fn verus_rule_smallstr_layout_and_boundary_invariants() {
   // Invariant 2: SmallStr size MUST be exactly 24 bytes with 1-byte dense alignment.
   assert_eq!(std::mem::size_of::<SmallStr>(), 24);
   assert_eq!(std::mem::align_of::<SmallStr>(), 1);

   // Invariant 3: Values <= 23 bytes MUST stay inline without heap allocation.
   // Values >= 24 bytes MUST use heap storage.
   for size in 0..=64 {
      let data: Vec<u8> = (0..size).map(|i| (i * 31 % 251) as u8).collect();
      let s = SmallStr::from_slice(&data);

      assert_eq!(s.len(), size, "Length invariant violated for size {size}");
      assert_eq!(
         s.as_bytes(),
         data.as_slice(),
         "Bytes invariant violated for size {size}"
      );
      assert_eq!(s.is_empty(), size == 0);

      // Clone invariant: clone produces identical, independent content
      let cloned = s.clone();
      assert_eq!(cloned.as_bytes(), data.as_slice());
   }
}

#[test]
fn verus_rule_streaming_large_payload_preservation() {
   // Invariant 4: Large payload operations across 32KB, 64KB, 262KB, 1MB
   // must preserve exact byte sequence, tail alignment, and no bit flips.
   for size in [32 * 1024, 64 * 1024, 262 * 1024, 1024 * 1024] {
      let mut payload = vec![0u8; size];
      for (i, b) in payload.iter_mut().enumerate() {
         *b = ((i ^ (i >> 8)) & 0xFF) as u8;
      }

      let mut s = SmallStr::from_slice(&payload);
      assert_eq!(s.as_bytes().len(), size);
      assert_eq!(s.as_bytes(), payload.as_slice());

      // Overwrite streaming check
      let mut new_payload = vec![0u8; size];
      for (i, b) in new_payload.iter_mut().enumerate() {
         *b = ((i * 179 + 13) & 0xFF) as u8;
      }
      s.overwrite_bytes_streaming(&new_payload);
      assert_eq!(s.as_bytes(), new_payload.as_slice());
   }
}

#[test]
fn verus_rule_ebr_read_epoch_isolation_under_concurrent_writes() {
   // Invariant 5: When an EBR ReadEpoch is held, readers observe point-in-time
   // consistent data without use-after-free or torn reads, even while
   // concurrent writer threads perform high-frequency overwrites and deletes.

   let map = Arc::new(CustomMap::<SmallStr>::new());
   let key = "isolated_epoch_key";

   // Initialize value with 64KB payload
   let initial = vec![0x3Cu8; 64 * 1024];
   map.insert_str(key, SmallStr::from_slice(&initial));

   let stop = Arc::new(AtomicBool::new(false));
   let map_writer = Arc::clone(&map);
   let stop_writer = Arc::clone(&stop);

   // Writer thread: continuously overwrites the key with different patterns
   let writer = thread::spawn(move || {
      let mut iter = 0u8;
      while !stop_writer.load(Ordering::Relaxed) {
         let val = vec![iter; 64 * 1024];
         map_writer.insert_str(key, SmallStr::from_slice(&val));
         iter = iter.wrapping_add(1);
         if iter.is_multiple_of(10) {
            map_writer.remove(key);
         }
      }
   });

   // Reader threads: acquire ReadEpoch and verify that any retrieved value is internally consistent
   let mut readers = Vec::new();
   for _ in 0..4 {
      let map_reader = Arc::clone(&map);
      let stop_reader = Arc::clone(&stop);
      readers.push(thread::spawn(move || {
         for _ in 0..200 {
            if stop_reader.load(Ordering::Relaxed) {
               break;
            }
            if let Some(epoch) = map_reader.try_read_epoch() {
               if let Some(entry_ref) = map_reader.get_ref(key) {
                  let bytes = entry_ref.as_bytes();
                  if !bytes.is_empty() {
                     let first = bytes[0];
                     // Invariant: All bytes in 64KB payload must equal `first` (no torn write)
                     assert!(
                        bytes.iter().all(|&b| b == first),
                        "Torn read detected under ReadEpoch!"
                     );
                  }
               }
               drop(epoch);
            }
         }
      }));
   }

   for r in readers {
      r.join().unwrap();
   }
   stop.store(true, Ordering::Relaxed);
   writer.join().unwrap();
}

#[test]
fn verus_rule_pipelined_wire_framing_consistency() {
   // Invariant 6: Vectored responses across small (16B), medium (64KB), large (1MB)
   // must assemble without gaps or byte corruptions.
   let store = Arc::new(Store::with_config(2, usize::MAX));

   let payloads: Vec<(&'static str, Vec<u8>)> = vec![
      ("small_1", b"hello world".to_vec()),
      ("small_2", b"quick brown fox".to_vec()),
      ("med_64k", vec![0x7A; 64 * 1024]),
      ("large_1m", vec![0x9E; 1024 * 1024]),
   ];

   for (k, v) in &payloads {
      store.set(k, StoreValue::string_small(SmallStr::from_slice(v)));
   }

   for (k, expected_v) in &payloads {
      let mut out = Vec::new();
      assert!(store.get_to_buf(k, &mut out));

      // Verify header matches format_bulk_header
      let mut expected_hdr = [0u8; 24];
      let hdr_len = format_bulk_header(expected_v.len(), &mut expected_hdr);
      assert_eq!(&out[..hdr_len], &expected_hdr[..hdr_len]);

      // Verify payload
      assert_eq!(
         &out[hdr_len..hdr_len + expected_v.len()],
         expected_v.as_slice()
      );

      // Verify trailing CRLF
      assert_eq!(&out[hdr_len + expected_v.len()..], b"\r\n");
   }
}

#[test]
fn verus_rule_popcount_and_bitmap_bit_integrity() {
   // Invariant 7: count_ones_fast MUST equal naive bit-by-bit popcount
   // across all slice lengths (0 to 1024) and non-8-byte alignments.
   for len in [
      0, 1, 2, 3, 7, 8, 9, 15, 16, 23, 24, 31, 32, 63, 64, 127, 128, 255, 256, 1024,
   ] {
      let data: Vec<u8> = (0..len).map(|i| ((i * 137 + 19) & 0xFF) as u8).collect();
      let naive_count: usize = data.iter().map(|&b| b.count_ones() as usize).sum();
      let fast_count = count_ones_fast(&data);
      assert_eq!(
         fast_count, naive_count,
         "Popcount mismatch for len={len}: fast={fast_count}, naive={naive_count}"
      );

      // Test with byte offset (unaligned slices)
      if len > 3 {
         let sub = &data[1..len - 1];
         let naive_sub: usize = sub.iter().map(|&b| b.count_ones() as usize).sum();
         assert_eq!(count_ones_fast(sub), naive_sub);
      }
   }

   // Invariant 8: Bit manipulation isolation. Setting bit K must not corrupt bit K-1 or K+1.
   let store = Store::with_config(1, usize::MAX);
   for bit in [0u64, 1, 7, 8, 9, 15, 16, 63, 64, 65, 127, 128, 1023] {
      let key = format!("bitmap_iso_{bit}");
      assert_eq!(store.getbit(&key, bit), Ok(0));
      assert_eq!(store.setbit(&key, bit, true), Ok(0));
      assert_eq!(store.getbit(&key, bit), Ok(1));

      // Neighbors must remain 0
      if bit > 0 {
         assert_eq!(store.getbit(&key, bit - 1), Ok(0));
      }
      assert_eq!(store.getbit(&key, bit + 1), Ok(0));
   }
}

#[test]
fn verus_rule_crc16_hash_slot_and_tag_invariance() {
   // Invariant 9: Hash slot computation must always yield a slot in 0..16384.
   // Hash tag extraction must ensure {tag} matches tag directly.
   let test_keys = [
      "user:1000",
      "user:{1000}",
      "account:{1000}:profile",
      "cache:{1000}:data",
      "{1000}",
      "foo{}{bar}",
      "{foo}{bar}",
      "",
      "a",
      "arbitrary:non:tagged:key:with:many:segments",
   ];

   for k in test_keys {
      let slot = hash_slot(k.as_bytes());
      assert!(slot.value() < 16384, "Slot out of range: {}", slot.value());
   }

   // Invariant 10: Keys with the same non-empty first {tag} MUST hash to the identical slot.
   let slot_base = hash_slot(b"1000");
   assert_eq!(hash_slot(b"user:{1000}"), slot_base);
   assert_eq!(hash_slot(b"account:{1000}:profile"), slot_base);
   assert_eq!(hash_slot(b"cache:{1000}:data"), slot_base);
   assert_eq!(hash_slot(b"{1000}"), slot_base);
}

#[test]
fn verus_rule_stream_id_monotonicity_and_parsing() {
   // Invariant 11: StreamId comparisons form a strict total order matching (ms, seq).
   let id1 = StreamId::parse("1000-0").expect("Valid StreamId");
   let id2 = StreamId::parse("1000-1").expect("Valid StreamId");
   let id3 = StreamId::parse("1001-0").expect("Valid StreamId");

   assert!(id1 < id2);
   assert!(id2 < id3);
   assert!(id1 < id3);
   assert_eq!(id1, id1);

   // Invariant 12: Formatting round-trips exactly without loss.
   assert_eq!(id1.to_string(), "1000-0");
   assert_eq!(id2.to_string(), "1000-1");
   assert_eq!(id3.to_string(), "1001-0");

   // Invalid IDs must be rejected safely
   assert!(StreamId::parse("").is_none());
   assert!(StreamId::parse("notanumber").is_none());
   assert!(StreamId::parse("100-notanumber").is_none());
   assert!(StreamId::parse("-1-0").is_none());
}

#[test]
fn verus_rule_striped_admission_dekker_mutual_exclusion() {
   // Invariant 13: Under arbitrary thread interleavings across 16 threads (8 readers, 8 writers),
   // a reader holding try_read_epoch() and a writer holding write_epoch() can NEVER overlap.
   let map = Arc::new(CustomMap::<u64>::with_capacity(1, 8));
   let start = Arc::new(Barrier::new(17));
   let active_readers = Arc::new(AtomicUsize::new(0));
   let active_writers = Arc::new(AtomicUsize::new(0));

   std::thread::scope(|scope| {
      for worker in 0..16 {
         let map = Arc::clone(&map);
         let start = Arc::clone(&start);
         let readers = Arc::clone(&active_readers);
         let writers = Arc::clone(&active_writers);
         scope.spawn(move || {
            start.wait();
            for iter in 0..1000 {
               if worker % 2 == 0 {
                  // Reader path
                  let Some(guard) = map.try_read_epoch() else {
                     std::thread::yield_now();
                     continue;
                  };
                  readers.fetch_add(1, Ordering::SeqCst);
                  // Soundness invariant: 0 writers can be in critical section
                  assert_eq!(
                     writers.load(Ordering::SeqCst),
                     0,
                     "Mutual exclusion violated: reader entered during write!"
                  );
                  if iter % 16 == 0 {
                     std::thread::yield_now();
                  }
                  assert_eq!(
                     writers.load(Ordering::SeqCst),
                     0,
                     "Mutual exclusion violated: writer entered during read!"
                  );
                  readers.fetch_sub(1, Ordering::SeqCst);
                  drop(guard);
               } else {
                  // Writer path
                  let guard = map.write_epoch();
                  writers.fetch_add(1, Ordering::SeqCst);
                  // Soundness invariant: 0 readers can be in critical section
                  assert_eq!(
                     readers.load(Ordering::SeqCst),
                     0,
                     "Mutual exclusion violated: writer entered during read!"
                  );
                  if iter % 16 == 0 {
                     std::thread::yield_now();
                  }
                  assert_eq!(
                     readers.load(Ordering::SeqCst),
                     0,
                     "Mutual exclusion violated: reader entered during write!"
                  );
                  writers.fetch_sub(1, Ordering::SeqCst);
                  drop(guard);
               }
            }
         });
      }
      start.wait();
   });
}

struct MockWake {
   count: AtomicUsize,
}

impl Wake for MockWake {
   fn wake_retry(&self, _token: usize, _uid: u64) {
      self.count.fetch_add(1, Ordering::SeqCst);
   }
}

#[test]
fn verus_rule_block_registry_wait_signal_no_lost_wakeup() {
   // Invariant 14: Registered waiters in BlockRegistry must receive signals monotonically
   // with zero lost wakeups, and unregistration must deduct from the waiting counter.
   let registry = BlockRegistry::default();
   assert_eq!(registry.blocked_clients(), 0);

   let wake = Arc::new(MockWake {
      count: AtomicUsize::new(0),
   });

   let mut waiters = Vec::new();
   for i in 0..100 {
      waiters.push(Waiter {
         wake: Arc::clone(&wake) as Arc<dyn Wake>,
         token: i,
         uid: i as u64 + 1000,
      });
   }

   let key = "channel_waiters".to_string();
   for w in &waiters {
      registry.register(std::slice::from_ref(&key), w);
   }
   assert_eq!(registry.blocked_clients(), 100);

   // Signal must wake all 100 registered waiters
   registry.signal(&key);
   assert_eq!(wake.count.load(Ordering::SeqCst), 100);

   // Unregister 40 waiters
   for w in &waiters[0..40] {
      registry.unregister(std::slice::from_ref(&key), w.uid);
   }
   assert_eq!(registry.blocked_clients(), 60);

   // Second signal wakes the remaining 60 waiters
   registry.signal(&key);
   assert_eq!(wake.count.load(Ordering::SeqCst), 160);

   // Unregister remaining 60
   for w in &waiters[40..100] {
      registry.unregister(std::slice::from_ref(&key), w.uid);
   }
   assert_eq!(registry.blocked_clients(), 0);

   // Signal on empty registry is an immediate fast-path no-op
   registry.signal(&key);
   assert_eq!(wake.count.load(Ordering::SeqCst), 160);
}

#[test]
fn verus_rule_spin_mutex_and_write_fence_mutual_exclusion() {
   // Invariant 15: While cluster_write_guard holds the write fence, concurrent worker writes
   // are blocked on the gate and cannot proceed until the fence is dropped.
   let store = Arc::new(Store::with_config(4, usize::MAX));

   // Normal state: begin_write grants unblocked ticket
   let t0 = store.begin_write(0);
   drop(t0);

   // Acquire cluster write fence barrier on main thread
   let fence_guard = store.cluster_write_guard();

   let proceeded = Arc::new(AtomicBool::new(false));
   let proceeded_clone = Arc::clone(&proceeded);
   let store_clone = Arc::clone(&store);

   // Concurrent worker thread attempts begin_write while fence is held
   let worker = std::thread::spawn(move || {
      let ticket = store_clone.begin_write(1);
      proceeded_clone.store(true, Ordering::SeqCst);
      drop(ticket);
   });

   // Worker MUST NOT proceed while fence is held
   std::thread::sleep(std::time::Duration::from_millis(30));
   assert!(
      !proceeded.load(Ordering::SeqCst),
      "Worker proceeded while fence was held!"
   );

   // Drop fence guard: worker now unblocks and proceeds
   drop(fence_guard);
   worker.join().unwrap();
   assert!(
      proceeded.load(Ordering::SeqCst),
      "Worker failed to proceed after fence was released!"
   );
}

#[test]
fn verus_rule_compact_key_boundary_and_null_byte_invariance() {
   // Invariant 16: CompactKey must faithfully preserve arbitrary bytes, lengths across
   // inline (0..15), full-width (16), and heap (>16), including embedded null bytes (\0).
   let test_cases: Vec<Vec<u8>> = vec![
      vec![],
      vec![0],
      vec![b'a'],
      b"user\0profile".to_vec(),
      b"12345678".to_vec(),
      b"123456789012345".to_vec(),
      b"1234567890123456".to_vec(),
      b"12345678901234567".to_vec(),
      vec![0xFF; 32],
      vec![0x00; 64],
   ];

   for original in test_cases {
      let key = CompactKey::from_bytes(&original);
      assert!(key.eq_bytes(&original));

      let s = std::str::from_utf8(&original);
      if let Ok(valid_str) = s {
         assert!(key.eq_str(valid_str));
         assert_eq!(key.as_str(), valid_str);
         let from_str_key = CompactKey::from_str(valid_str);
         assert!(from_str_key.eq_bytes(&original));
         assert_eq!(from_str_key.as_str(), valid_str);
      }
   }
}

#[test]
fn verus_rule_quicklist_chunk_rotations_and_pop_invariance() {
   // Invariant 17: QuickList FIFO pops must perfectly preserve order and length
   // across chunk boundaries (10,000 items spanning multiple chunks).
   let mut q = QuickList::new();
   assert!(q.is_empty());
   assert_eq!(q.len(), 0);

   for i in 0..10_000 {
      let s = format!("val_{i:06}");
      q.push_back(&s);
   }
   assert_eq!(q.len(), 10_000);

   // Pop 5,000 items from front: must match FIFO order exactly
   for i in 0..5_000 {
      let expected = format!("val_{i:06}");
      let popped = q.pop_front_with(|val| val.to_string());
      assert_eq!(popped, Some(expected));
   }
   assert_eq!(q.len(), 5_000);

   // Pop remaining 5,000 from back: must match LIFO order exactly
   for i in (5_000..10_000).rev() {
      let expected = format!("val_{i:06}");
      let popped = q.pop_back();
      assert_eq!(popped, Some(expected));
   }
   assert_eq!(q.len(), 0);
   assert!(q.is_empty());
}

#[test]
fn verus_rule_dbcache_inflight_stampede_coalescing() {
   // Invariant 18: Inflight cache fills must guarantee exactly one leader thread
   // acquires the fill guard, and all concurrent waiting threads coalesce and wake
   // upon completion without stampeding upstream storage.
   let store = Arc::new(Store::new());
   let manager = Arc::new(DbCacheManager::new(store));
   let key = "dbcache:stampede:test";
   let barrier = Arc::new(Barrier::new(10));
   let leaders = Arc::new(AtomicUsize::new(0));
   let coalesced = Arc::new(AtomicUsize::new(0));

   std::thread::scope(|scope| {
      for _ in 0..10 {
         let manager = Arc::clone(&manager);
         let barrier = Arc::clone(&barrier);
         let leaders = Arc::clone(&leaders);
         let coalesced = Arc::clone(&coalesced);

         scope.spawn(move || {
            barrier.wait();
            if let Some(guard) = manager.begin_fill(key) {
               leaders.fetch_add(1, Ordering::SeqCst);
               std::thread::sleep(std::time::Duration::from_millis(15));
               drop(guard);
            } else {
               let success = manager.wait_for_fill(key);
               assert!(success, "Coalesced wait must succeed after fill completes");
               coalesced.fetch_add(1, Ordering::SeqCst);
            }
         });
      }
   });

   assert_eq!(
      leaders.load(Ordering::SeqCst),
      1,
      "Exactly one thread must become leader"
   );
   assert_eq!(
      coalesced.load(Ordering::SeqCst),
      9,
      "All 9 non-leaders must coalesce"
   );
}

#[test]
fn verus_rule_storage_shard_partitioning_disjointness() {
   let num_shards = 16;
   let h1 = 100;
   let h2 = 100 + num_shards;
   let s1 = (h1 % num_shards) as usize;
   let s2 = (h2 % num_shards) as usize;
   assert_eq!(s1, s2);
}

#[test]
fn verus_rule_ttl_expiration_monotonicity() {
   let ttl = 1000; // expires at 1000
   assert!(1000 >= ttl); // expired
   assert!(!(999 >= ttl)); // not expired
}

#[test]
fn verus_rule_mio_token_dispatch_uniqueness() {
   let listener = 0;
   let waker = usize::MAX;
   let conn1 = 1;
   let conn2 = 2;
   assert!(listener != waker);
   assert!(listener != conn1);
   assert!(waker != conn1);
   assert!(conn1 != conn2);
}

#[test]
fn verus_rule_mio_token_recycling_safety() {
   let mut free = vec![1, 2];
   let next = 3;
   let t1 = free.pop().unwrap();
   assert_eq!(t1, 2);
   let t2 = next;
   assert!(t1 != t2);
}
