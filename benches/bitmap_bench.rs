//! Micro-benchmarks for bitmap and bitfield paths. Run with: cargo bench --bench bitmap_bench

use fyro_db::commands::bitmap::bitfield;
use fyro_db::storage::bitmap::BitOp;
use fyro_db::storage::store::Store;
use std::hint::black_box;
use std::time::{Duration, Instant};

const SMALL_STR_CAP: u64 = 23;

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
      "  {:<42} {:>10} {:>16}",
      label,
      format!("{}ms", elapsed.as_millis()),
      fmt_rate(rate)
   );
}

fn bench_setbit() {
   const OPS: u64 = 250_000;

   let store = Store::new();
   store.setbit("setbit:small", 7, false).unwrap();
   let start = Instant::now();
   for i in 0..OPS {
      black_box(store.setbit("setbit:small", 7, i & 1 != 0).unwrap());
   }
   print_bench("SETBIT small inline value", OPS, start.elapsed());

   let large_offset = SMALL_STR_CAP * 8 + 1;
   store.setbit("setbit:large", large_offset, false).unwrap();
   let start = Instant::now();
   for i in 0..OPS {
      black_box(
         store
            .setbit("setbit:large", large_offset, i & 1 != 0)
            .unwrap(),
      );
   }
   print_bench("SETBIT large heap value", OPS, start.elapsed());

   const FRESH_OPS: u64 = 25_000;
   let fresh_store = Store::with_config(1, FRESH_OPS as usize);
   let keys: Vec<String> = (0..FRESH_OPS)
      .map(|i| format!("setbit:fresh:{i}"))
      .collect();
   let start = Instant::now();
   for key in &keys {
      black_box(fresh_store.setbit(key, 7, true).unwrap());
   }
   print_bench("SETBIT fresh key growth", FRESH_OPS, start.elapsed());
}

fn bench_getbit() {
   const OPS: u64 = 500_000;
   let store = Store::new();
   store.setbit("getbit:small", 7, true).unwrap();
   store
      .setbit("getbit:large", SMALL_STR_CAP * 8 + 1, true)
      .unwrap();

   let start = Instant::now();
   for _ in 0..OPS {
      black_box(store.getbit("getbit:small", 7).unwrap());
   }
   print_bench("GETBIT small inline value", OPS, start.elapsed());

   let start = Instant::now();
   for _ in 0..OPS {
      black_box(store.getbit("getbit:large", SMALL_STR_CAP * 8 + 1).unwrap());
   }
   print_bench("GETBIT large heap value", OPS, start.elapsed());
}

fn bench_bitfield_store() {
   const OPS: u64 = 250_000;
   for (label, bits, signed) in [
      ("u8", 8, false),
      ("i16", 16, true),
      ("u32", 32, false),
      ("i64", 64, true),
   ] {
      for (alignment, offset) in [("aligned", 0), ("unaligned", 1)] {
         let key = format!("bf:{label}:{alignment}");
         let store = Store::new();
         store.bf_set(&key, offset, bits, 123).unwrap();

         let start = Instant::now();
         for _ in 0..OPS {
            black_box(store.bf_get(&key, offset, bits, signed).unwrap());
         }
         print_bench(
            &format!("BITFIELD GET {label} {alignment}"),
            OPS,
            start.elapsed(),
         );

         let start = Instant::now();
         for i in 0..OPS {
            black_box(store.bf_set(&key, offset, bits, (i & 0x7f) as i64).unwrap());
         }
         print_bench(
            &format!("BITFIELD SET {label} {alignment}"),
            OPS,
            start.elapsed(),
         );
      }
   }
}

fn bench_bitfield_command() {
   const OPS: u64 = 100_000;
   let store = Store::new();
   let mut out = Vec::with_capacity(64);
   let get = ["BITFIELD", "bf:command:get", "GET", "u8", "0"];
   let set = ["BITFIELD", "bf:command:set", "SET", "u8", "8", "200"];
   let incrby = [
      "BITFIELD",
      "bf:command:incrby",
      "OVERFLOW",
      "SAT",
      "INCRBY",
      "i16",
      "0",
      "1",
   ];

   let start = Instant::now();
   for _ in 0..OPS {
      out.clear();
      bitfield(black_box(&get), &store, &mut out);
      black_box(out.as_slice());
   }
   print_bench("BITFIELD command GET", OPS, start.elapsed());

   let start = Instant::now();
   for _ in 0..OPS {
      out.clear();
      bitfield(black_box(&set), &store, &mut out);
      black_box(out.as_slice());
   }
   print_bench("BITFIELD command SET", OPS, start.elapsed());

   let start = Instant::now();
   for _ in 0..OPS {
      out.clear();
      bitfield(black_box(&incrby), &store, &mut out);
      black_box(out.as_slice());
   }
   print_bench("BITFIELD command INCRBY OVERFLOW SAT", OPS, start.elapsed());
}

fn bitop_name(op: BitOp) -> &'static str {
   match op {
      BitOp::And => "AND",
      BitOp::Or => "OR",
      BitOp::Xor => "XOR",
      BitOp::Not => "NOT",
   }
}

fn bench_bitop_case(op: BitOp, source_count: usize, max_len: usize) {
   let ops = if max_len == 1_024 { 100_000 } else { 5_000 };
   let store = Store::with_config(1, source_count + 1);
   let keys: Vec<String> = (0..source_count)
      .map(|i| format!("bitop:src:{i}"))
      .collect();
   for (i, key) in keys.iter().enumerate() {
      let len = if source_count == 1 || i == 0 {
         max_len
      } else if i % 2 == 0 {
         max_len / 4
      } else {
         max_len / 2
      };
      let bytes: Vec<u8> = (0..len).map(|j| b'a' + ((j + i) % 26) as u8).collect();
      let value = String::from_utf8(bytes).unwrap();
      store.set_string(key, &value, 0);
   }
   let source_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
   let label = format!(
      "BITOP {} {} sources (max {}KiB, mixed lengths)",
      bitop_name(op),
      source_count,
      max_len / 1024
   );

   let start = Instant::now();
   for _ in 0..ops {
      black_box(store.bitop(op, "bitop:dst", &source_refs).unwrap());
   }
   print_bench(&label, ops, start.elapsed());
}

fn bench_bitop() {
   for max_len in [1_024usize, 65_536] {
      for source_count in [2usize, 8, 32] {
         for op in [BitOp::And, BitOp::Or, BitOp::Xor] {
            bench_bitop_case(op, source_count, max_len);
         }
      }
      bench_bitop_case(BitOp::Not, 1, max_len);
   }
}

fn main() {
   println!("============================================================");
   println!("               FyroDB Bitmap Micro-Benchmarks               ");
   println!("============================================================");
   println!("── Summary ─────────────────────────────────────────────────────");
   println!("  Benchmark                                    Elapsed       Throughput");
   println!("  ────────────────────────────────────────── ────────── ────────────────");

   bench_setbit();
   bench_getbit();
   bench_bitfield_store();
   bench_bitfield_command();
   bench_bitop();
}
