//! Cluster hot-path benchmarks (mutation log, routing, frame codec, slot scans).
//! Run with: cargo bench --bench cluster_perf

use fyro_db::cluster::{
   Frame, FrameCodec, MessageType, MutationKind, MutationLog, MutationRecord, Slot, hash_slot,
   routing_scope,
};
use fyro_db::storage::store::Store;
use std::hint::black_box;
use std::io::Cursor;
use std::time::{Duration, Instant};

fn fmt_rate(rate: f64) -> String {
   if rate >= 1e9 {
      format!("{:.2}B ops/sec", rate / 1e9)
   } else if rate >= 1e6 {
      format!("{:.2}M ops/sec", rate / 1e6)
   } else if rate >= 1e3 {
      format!("{:.1}k ops/sec", rate / 1e3)
   } else {
      format!("{:.0} ops/sec", rate)
   }
}

fn print_bench(label: &str, ops: u64, elapsed: Duration) {
   let rate = ops as f64 / elapsed.as_secs_f64();
   println!(
      "  {:<36} {:>10} {:>16}",
      label,
      format!("{}ms", elapsed.as_millis()),
      fmt_rate(rate)
   );
}

fn bench_mutation_log() {
   const ENTRIES: u64 = 100_000;
   let mut log = MutationLog::new(ENTRIES as usize);
   for index in 0..ENTRIES {
      log.append(MutationRecord {
         offset: 0,
         slot: Slot(1),
         kind: MutationKind::Replace,
         key: format!("key:{index}").into_bytes(),
         value: vec![0u8; 64],
         expire_at_ms: None,
      })
      .unwrap();
   }

   const OPS: u64 = 500_000;
   let start = Instant::now();
   for _ in 0..OPS {
      let record = log.record_at(black_box(1u64)).unwrap().unwrap();
      black_box(record.offset);
   }
   print_bench("mutation_log (head record_at)", OPS, start.elapsed());

   let start = Instant::now();
   for _ in 0..OPS {
      let record = log.record_at(black_box(ENTRIES)).unwrap().unwrap();
      black_box(record.offset);
   }
   print_bench("mutation_log (tail record_at)", OPS, start.elapsed());
}

fn bench_routing() {
   const OPS: u64 = 2_000_000;
   for command in [
      b"GET".as_slice(),
      b"SET".as_slice(),
      b"ZRANGE".as_slice(),
      b"MSET".as_slice(),
      b"PING".as_slice(),
   ] {
      let label = std::str::from_utf8(command).unwrap();
      let start = Instant::now();
      for _ in 0..OPS {
         black_box(routing_scope(black_box(command)));
      }
      print_bench(&format!("routing_scope ({label})"), OPS, start.elapsed());
   }
}

fn bench_frame_codec() {
   let codec = FrameCodec::default();
   for size in [64usize, 4096, 65_536] {
      let frame = Frame {
         message_type: MessageType::ReplicationEntry,
         flags: 0,
         request_id: 42,
         source_id: 1,
         target_id: 2,
         epoch: 1,
         payload: vec![0xab; size],
      };

      const OPS: u64 = 50_000;
      let mut wire = Vec::with_capacity(size + 64);
      let start = Instant::now();
      for _ in 0..OPS {
         wire.clear();
         codec.write_frame(&mut wire, black_box(&frame)).expect("encode");
         black_box(wire.len());
      }
      print_bench(&format!("frame_codec encode ({size}B)"), OPS, start.elapsed());

      let mut wire = Vec::with_capacity(size + 64);
      codec.write_frame(&mut wire, &frame).expect("encode");
      let start = Instant::now();
      for _ in 0..OPS {
         let mut cursor = Cursor::new(black_box(wire.as_slice()));
         let frame = codec.read_frame(&mut cursor).expect("decode");
         black_box(frame.payload.len());
      }
      print_bench(&format!("frame_codec decode ({size}B)"), OPS, start.elapsed());
   }
}

fn bench_write_fence() {
   let store = Store::with_config_workers(4, usize::MAX, 4);
   const OPS: u64 = 2_000_000;
   let start = Instant::now();
   for _ in 0..OPS {
      let ticket = store.begin_write(black_box(0));
      black_box(&ticket);
   }
   print_bench("write_fence/begin_write", OPS, start.elapsed());
}

fn bench_slot_scan() {
   const KEYS: usize = 200_000;
   let store = Store::with_config_workers(16, usize::MAX, 1);
   for index in 0..KEYS {
      store.set_string(&format!("bench:{index}"), "x", 0);
   }
   let slot = hash_slot(b"bench:0");

   const COUNT_OPS: u64 = 500;
   let start = Instant::now();
   for _ in 0..COUNT_OPS {
      black_box(store.count_keys_in_slot(black_box(slot)));
   }
   print_bench("slot_scan count_keys_in_slot", COUNT_OPS, start.elapsed());

   const KEYS_OPS: u64 = 500;
   let start = Instant::now();
   for _ in 0..KEYS_OPS {
      black_box(store.keys_in_slot(black_box(slot), 100).len());
   }
   print_bench("slot_scan keys_in_slot(100)", KEYS_OPS, start.elapsed());
}

fn main() {
   println!("============================================================");
   println!("           FyroDB Cluster Micro-Benchmarks                  ");
   println!("============================================================");
   println!("── Summary ─────────────────────────────────────────────────────");
   println!("  Benchmark                               Elapsed       Throughput");
   println!("  ──────────────────────────────────── ────────── ────────────────");

   bench_mutation_log();
   bench_routing();
   bench_frame_codec();
   bench_write_fence();
   bench_slot_scan();
}
