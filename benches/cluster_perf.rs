//! Cluster hot-path benchmarks (mutation log, routing, frame codec, slot scans).
//! Run with: cargo bench --bench cluster_perf

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use fyro_db::cluster::{
   Frame, FrameCodec, MessageType, MutationKind, MutationLog, MutationRecord, Slot, hash_slot,
   routing_scope,
};
use fyro_db::storage::store::Store;
use std::hint::black_box;
use std::io::Cursor;

fn bench_mutation_log(c: &mut Criterion) {
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
   let mut group = c.benchmark_group("mutation_log_record_at");
   for (label, offset) in [("head", 1u64), ("tail", ENTRIES)] {
      group.bench_with_input(BenchmarkId::from_parameter(label), &offset, |b, &offset| {
         b.iter(|| {
            let record = log.record_at(black_box(offset)).unwrap().unwrap();
            black_box(record.offset)
         });
      });
   }
   group.finish();
}

fn bench_routing(c: &mut Criterion) {
   let mut group = c.benchmark_group("routing_scope");
   for command in [
      b"GET".as_slice(),
      b"SET".as_slice(),
      b"ZRANGE".as_slice(),
      b"MSET".as_slice(),
      b"PING".as_slice(),
   ] {
      let label = std::str::from_utf8(command).unwrap();
      group.bench_with_input(
         BenchmarkId::from_parameter(label),
         &command,
         |b, &command| b.iter(|| black_box(routing_scope(black_box(command)))),
      );
   }
   group.finish();
}

fn bench_frame_codec(c: &mut Criterion) {
   let codec = FrameCodec::default();
   let mut group = c.benchmark_group("frame_codec");
   for size in [64usize, 4096, 65_536] {
      group.throughput(Throughput::Bytes(size as u64));
      let frame = Frame {
         message_type: MessageType::ReplicationEntry,
         flags: 0,
         request_id: 42,
         source_id: 1,
         target_id: 2,
         epoch: 1,
         payload: vec![0xab; size],
      };
      group.bench_with_input(BenchmarkId::new("encode", size), &frame, |b, frame| {
         let mut wire = Vec::with_capacity(size + 64);
         b.iter(|| {
            wire.clear();
            codec
               .write_frame(&mut wire, black_box(frame))
               .expect("encode");
            black_box(wire.len())
         });
      });
      let mut wire = Vec::with_capacity(size + 64);
      codec.write_frame(&mut wire, &frame).expect("encode");
      group.bench_with_input(BenchmarkId::new("decode", size), &wire, |b, wire| {
         b.iter(|| {
            let mut cursor = Cursor::new(black_box(wire.as_slice()));
            let frame = codec.read_frame(&mut cursor).expect("decode");
            black_box(frame.payload.len())
         });
      });
   }
   group.finish();
}

fn bench_write_fence(c: &mut Criterion) {
   let store = Store::with_config_workers(4, usize::MAX, 4);
   c.bench_function("write_fence/begin_write", |b| {
      b.iter(|| {
         let ticket = store.begin_write(black_box(0));
         black_box(&ticket);
      });
   });
}

fn bench_slot_scan(c: &mut Criterion) {
   const KEYS: usize = 200_000;
   let store = Store::with_config_workers(16, usize::MAX, 1);
   for index in 0..KEYS {
      store.set_string(&format!("bench:{index}"), "x", 0);
   }
   let slot = hash_slot(b"bench:0");
   let mut group = c.benchmark_group("slot_scan");
   group.bench_function("count_keys_in_slot", |b| {
      b.iter(|| black_box(store.count_keys_in_slot(black_box(slot))));
   });
   group.bench_function("get_100_keys", |b| {
      b.iter(|| black_box(store.keys_in_slot(black_box(slot), 100).len()));
   });
   group.finish();
}

criterion_group!(
   benches,
   bench_mutation_log,
   bench_routing,
   bench_frame_codec,
   bench_write_fence,
   bench_slot_scan
);
criterion_main!(benches);
