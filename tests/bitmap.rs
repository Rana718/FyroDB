mod common;
use common::*;
use fyro_db::storage::bitmap::BitOp;

#[test]
fn setbit_creates_key() {
   let s = store();
   assert_eq!(s.setbit("k", 7, true), Ok(0));
   assert_eq!(s.getbit("k", 7), Ok(1));
}

#[test]
fn setbit_returns_old_value() {
   let s = store();
   s.setbit("k", 7, true).unwrap();
   assert_eq!(s.setbit("k", 7, false), Ok(1));
   assert_eq!(s.getbit("k", 7), Ok(0));
}

#[test]
fn getbit_missing_key() {
   let s = store();
   assert_eq!(s.getbit("k", 100), Ok(0));
}

#[test]
fn getbit_beyond_length() {
   let s = store();
   s.setbit("k", 0, true).unwrap();
   assert_eq!(s.getbit("k", 100), Ok(0));
}

#[test]
fn setbit_expands_string() {
   let s = store();
   assert_eq!(s.setbit("k", 32, true), Ok(0));
   assert_eq!(s.getbit("k", 32), Ok(1));
   assert_eq!(s.getbit("k", 0), Ok(0));
}

#[test]
fn bitcount_full_string() {
   let s = store();
   s.setbit("k", 0, true).unwrap();
   s.setbit("k", 1, true).unwrap();
   s.setbit("k", 7, true).unwrap();
   assert_eq!(s.bitcount("k", 0, -1, false), Ok(3));
}

#[test]
fn bitcount_byte_range() {
   let s = store();
   for i in 0..8 {
      s.setbit("k", i, true).unwrap();
   }
   s.setbit("k", 8, true).unwrap();
   assert_eq!(s.bitcount("k", 0, 0, false), Ok(8));
   assert_eq!(s.bitcount("k", 1, 1, false), Ok(1));
}

#[test]
fn bitcount_bit_mode() {
   let s = store();
   s.setbit("k", 0, true).unwrap();
   s.setbit("k", 2, true).unwrap();
   s.setbit("k", 4, true).unwrap();
   assert_eq!(s.bitcount("k", 0, 4, true), Ok(3));
   assert_eq!(s.bitcount("k", 0, 1, true), Ok(1));
}

#[test]
fn bitcount_missing_key() {
   let s = store();
   assert_eq!(s.bitcount("k", 0, -1, false), Ok(0));
}

#[test]
fn bitpos_find_first_set() {
   let s = store();
   s.setbit("k", 10, true).unwrap();
   assert_eq!(s.bitpos("k", true, 0, -1, false, false), Ok(10));
}

#[test]
fn bitpos_find_first_clear() {
   let s = store();
   for i in 0..8 {
      s.setbit("k", i, true).unwrap();
   }
   assert_eq!(s.bitpos("k", false, 0, -1, false, false), Ok(8));
}

#[test]
fn bitpos_not_found() {
   let s = store();
   assert_eq!(s.bitpos("k", true, 0, -1, false, false), Ok(-1));
}

#[test]
fn bitop_and_wrong_type() {
  let s = store();
  s.setbit("a", 0, true).unwrap();
  s.setbit("a", 3, true).unwrap();
  s.lpush("b", &[&"hello"]).unwrap();
  let r = s.bitop(BitOp::And, "dst", &["a", "b"]);
  assert_eq!(r, Err("WRONGTYPE"));
}

#[test]
fn bitop_or() {
   let s = store();
   s.setbit("a", 0, true).unwrap();
   s.setbit("b", 1, true).unwrap();
   let len = s.bitop(BitOp::Or, "dst", &["a", "b"]).unwrap();
   assert_eq!(len, 1);
   assert_eq!(s.getbit("dst", 0), Ok(1));
   assert_eq!(s.getbit("dst", 1), Ok(1));
}

#[test]
fn bitop_and() {
   let s = store();
   s.setbit("a", 0, true).unwrap();
   s.setbit("a", 1, true).unwrap();
   s.setbit("b", 0, true).unwrap();
   let len = s.bitop(BitOp::And, "dst", &["a", "b"]).unwrap();
   assert_eq!(len, 1);
   assert_eq!(s.getbit("dst", 0), Ok(1));
   assert_eq!(s.getbit("dst", 1), Ok(0));
}

#[test]
fn bitop_xor() {
   let s = store();
   s.setbit("a", 0, true).unwrap();
   s.setbit("a", 1, true).unwrap();
   s.setbit("b", 1, true).unwrap();
   s.bitop(BitOp::Xor, "dst", &["a", "b"]).unwrap();
   assert_eq!(s.getbit("dst", 0), Ok(1));
   assert_eq!(s.getbit("dst", 1), Ok(0));
}

#[test]
fn bitop_not() {
   let s = store();
   s.setbit("a", 0, true).unwrap();
   s.bitop(BitOp::Not, "dst", &["a"]).unwrap();
   assert_eq!(s.getbit("dst", 0), Ok(0));
   assert_eq!(s.getbit("dst", 1), Ok(1));
}

#[test]
fn bitop_different_lengths() {
   let s = store();
   let a = [
      0xF0, 0x0F, 0xAA, 0x55, 0x81, 0x7E, 0x33, 0xCC, 0xA5, 0x5A, 0xFF,
   ];
   let b = [0x0F, 0xF0, 0x0F, 0xF0, 0x81, 0x00, 0xFF, 0x00, 0xFF];
   let set_bytes = |key: &str, bytes: &[u8]| {
      for (byte_idx, byte) in bytes.iter().enumerate() {
         for bit_idx in 0..8 {
            s.setbit(
               key,
               (byte_idx * 8 + bit_idx) as u64,
               *byte & (1 << (7 - bit_idx)) != 0,
            )
            .unwrap();
         }
      }
   };
   let assert_bytes = |key: &str, expected: &[u8]| {
      for (byte_idx, byte) in expected.iter().enumerate() {
         for bit_idx in 0..8 {
            assert_eq!(
               s.getbit(key, (byte_idx * 8 + bit_idx) as u64),
               Ok((byte >> (7 - bit_idx)) & 1)
            );
         }
      }
   };

   set_bytes("a", &a);
   set_bytes("b", &b);

   assert_eq!(s.bitop(BitOp::And, "dst", &["a", "b"]), Ok(11));
   assert_bytes(
      "dst",
      &[
         0x00, 0x00, 0x0A, 0x50, 0x81, 0x00, 0x33, 0x00, 0xA5, 0x00, 0x00,
      ],
   );

   assert_eq!(s.bitop(BitOp::Or, "dst", &["a", "b"]), Ok(11));
   assert_bytes(
      "dst",
      &[
         0xFF, 0xFF, 0xAF, 0xF5, 0x81, 0x7E, 0xFF, 0xCC, 0xFF, 0x5A, 0xFF,
      ],
   );

   assert_eq!(s.bitop(BitOp::Xor, "dst", &["a", "b"]), Ok(11));
   assert_bytes(
      "dst",
      &[
         0xFF, 0xFF, 0xA5, 0xA5, 0x00, 0x7E, 0xCC, 0xCC, 0x5A, 0x5A, 0xFF,
      ],
   );

   assert_eq!(s.bitop(BitOp::Not, "dst", &["a"]), Ok(11));
   assert_bytes(
      "dst",
      &[
         0x0F, 0xF0, 0x55, 0xAA, 0x7E, 0x81, 0xCC, 0x33, 0x5A, 0xA5, 0x00,
      ],
   );
}

#[test]
fn bitop_sources_grow_result() {
   let s = store();
   let short = [0xF0, 0x0F];
   let long = [
      0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xA0, 0xB0,
   ];
   let set_bytes = |key: &str, bytes: &[u8]| {
      for (byte_idx, byte) in bytes.iter().enumerate() {
         for bit_idx in 0..8 {
            s.setbit(
               key,
               (byte_idx * 8 + bit_idx) as u64,
               *byte & (1 << (7 - bit_idx)) != 0,
            )
            .unwrap();
         }
      }
   };
   let assert_bytes = |key: &str, expected: &[u8]| {
      for (byte_idx, byte) in expected.iter().enumerate() {
         for bit_idx in 0..8 {
            assert_eq!(
               s.getbit(key, (byte_idx * 8 + bit_idx) as u64),
               Ok((byte >> (7 - bit_idx)) & 1)
            );
         }
      }
   };

   set_bytes("short", &short);
   set_bytes("long", &long);

   assert_eq!(s.bitop(BitOp::And, "dst", &["short", "long"]), Ok(11));
   assert_bytes("dst", &[0x10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

   assert_eq!(s.bitop(BitOp::Or, "dst", &["short", "long"]), Ok(11));
   assert_bytes(
      "dst",
      &[
         0xF0, 0x2F, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xA0, 0xB0,
      ],
   );

   assert_eq!(s.bitop(BitOp::Xor, "dst", &["short", "long"]), Ok(11));
   assert_bytes(
      "dst",
      &[
         0xE0, 0x2F, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xA0, 0xB0,
      ],
   );

   assert_eq!(s.bitop(BitOp::Or, "dst", &["long", "short"]), Ok(11));
   assert_bytes(
      "dst",
      &[
         0xF0, 0x2F, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xA0, 0xB0,
      ],
   );

   assert_eq!(s.bitop(BitOp::And, "dst", &["missing", "long"]), Ok(11));
   assert_bytes("dst", &[0; 11]);

   assert_eq!(s.bitop(BitOp::Or, "dst", &["missing", "long"]), Ok(11));
   assert_bytes("dst", &long);

   assert_eq!(s.bitop(BitOp::Xor, "dst", &["missing", "long"]), Ok(11));
   assert_bytes("dst", &long);

   assert_eq!(s.bitop(BitOp::Xor, "long", &["long", "short"]), Ok(11));
   assert_bytes(
      "long",
      &[
         0xE0, 0x2F, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xA0, 0xB0,
      ],
   );
}

#[test]
fn bitop_not_wrong_type_precedes_arity_error() {
   let s = store();
   s.sadd("wrong", &["member"]).unwrap();

   assert_eq!(
      s.bitop(BitOp::Not, "dst", &["missing", "wrong"]),
      Err("WRONGTYPE")
   );
   assert_eq!(
      s.bitop(BitOp::Not, "dst", &["missing", "missing2"]),
      Err("BITOP NOT requires one source key")
   );
}

#[test]
fn setbit_wrong_type() {
   let s = store();
   s.sadd("k", &["member"]).unwrap();
   assert_eq!(s.setbit("k", 0, true), Err("WRONGTYPE"));
}

#[test]
fn getbit_wrong_type() {
   let s = store();
   s.sadd("k", &["member"]).unwrap();
   assert_eq!(s.getbit("k", 0), Err("WRONGTYPE"));
}

#[test]
fn bf_get_missing_key() {
   let s = store();
   assert_eq!(s.bf_get("k", 0, 8, false), Ok(0));
   assert_eq!(s.bf_get("k", 100, 16, true), Ok(0));
}

#[test]
fn bf_set_and_get_basic() {
   let s = store();
   s.bf_set("k", 0, 8, 0xAB).unwrap();
   assert_eq!(s.bf_get("k", 0, 8, false), Ok(0xAB));
   // 0xAB = 10101011 in binary
   assert_eq!(s.getbit("k", 0), Ok(1));
   assert_eq!(s.getbit("k", 1), Ok(0));
   assert_eq!(s.getbit("k", 2), Ok(1));
   assert_eq!(s.getbit("k", 3), Ok(0));
   assert_eq!(s.getbit("k", 4), Ok(1));
   assert_eq!(s.getbit("k", 5), Ok(0));
   assert_eq!(s.getbit("k", 6), Ok(1));
   assert_eq!(s.getbit("k", 7), Ok(1));
}

#[test]
fn bf_get_signed_sign_extension() {
   let s = store();
   s.bf_set("k", 0, 8, 0xFF).unwrap();
   assert_eq!(s.bf_get("k", 0, 8, true), Ok(-1));
   assert_eq!(s.bf_get("k", 0, 8, false), Ok(255));

   s.bf_set("k", 8, 4, 0b1111).unwrap();
   assert_eq!(s.bf_get("k", 8, 4, true), Ok(-1));
   assert_eq!(s.bf_get("k", 8, 4, false), Ok(15));

   s.bf_set("k", 12, 4, 0b0111).unwrap();
   assert_eq!(s.bf_get("k", 12, 4, true), Ok(7));
   assert_eq!(s.bf_get("k", 12, 4, false), Ok(7));
}

#[test]
fn bf_set_and_get_unaligned() {
   let s = store();
   s.bf_set("k", 4, 8, 0xAB).unwrap();
   assert_eq!(s.bf_get("k", 4, 8, false), Ok(0xAB));
   assert_eq!(s.bf_get("k", 0, 4, false), Ok(0));

   s.bf_set("k", 2, 13, 0x1ABC).unwrap();
   assert_eq!(s.bf_get("k", 2, 13, false), Ok(0x1ABC));
}

#[test]
fn bf_set_and_get_64bit() {
   let s = store();
   let val = 0x0123456789ABCDEF_i64;
   s.bf_set("k", 0, 64, val).unwrap();
   assert_eq!(s.bf_get("k", 0, 64, false), Ok(val));
   assert_eq!(s.bf_get("k", 0, 64, true), Ok(val));

   s.bf_set("k", 3, 64, -123456789012345678_i64).unwrap();
   assert_eq!(s.bf_get("k", 3, 64, true), Ok(-123456789012345678_i64));
}

#[test]
fn bf_set_and_get_single_bit() {
   let s = store();
   s.bf_set("k", 10, 1, 1).unwrap();
   assert_eq!(s.bf_get("k", 10, 1, false), Ok(1));
   assert_eq!(s.getbit("k", 10), Ok(1));

   s.bf_set("k", 10, 1, 0).unwrap();
   assert_eq!(s.bf_get("k", 10, 1, false), Ok(0));
   assert_eq!(s.getbit("k", 10), Ok(0));
}

#[test]
fn bf_wrong_type() {
   let s = store();
   s.sadd("k", &["member"]).unwrap();
   assert_eq!(s.bf_get("k", 0, 8, false), Err("WRONGTYPE"));
   assert_eq!(s.bf_set("k", 0, 8, 42), Err("WRONGTYPE"));
}

#[test]
fn bf_edge_cases() {
   let s = store();
   assert_eq!(s.bf_get("k", 0, 0, false), Ok(0));
   assert_eq!(s.bf_set("k", 0, 0, 123), Ok(()));
   assert_eq!(s.bf_get("k", 0, 65, false), Err("invalid bitfield type"));
   assert_eq!(s.bf_set("k", 0, 65, 123), Err("invalid bitfield type"));
}

#[test]
fn bitfield_command_get_set_incrby() {
   let s = store();
   let mut out = Vec::new();
   fyro_db::commands::bitmap::bitfield(&["BITFIELD", "k", "SET", "u8", "0", "200"], &s, &mut out);
   assert_eq!(String::from_utf8_lossy(&out), "*1\r\n:0\r\n");

   out.clear();
   fyro_db::commands::bitmap::bitfield(&["BITFIELD", "k", "GET", "u8", "0"], &s, &mut out);
   assert_eq!(String::from_utf8_lossy(&out), "*1\r\n:200\r\n");

   out.clear();
   fyro_db::commands::bitmap::bitfield(&["BITFIELD", "k", "INCRBY", "u8", "0", "10"], &s, &mut out);
   assert_eq!(String::from_utf8_lossy(&out), "*1\r\n:210\r\n");

   out.clear();
   fyro_db::commands::bitmap::bitfield(
      &[
         "BITFIELD", "k", "OVERFLOW", "SAT", "INCRBY", "u8", "0", "100",
      ],
      &s,
      &mut out,
   );
   assert_eq!(String::from_utf8_lossy(&out), "*1\r\n:255\r\n");
}
