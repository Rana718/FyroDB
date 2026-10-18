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
   assert_eq!(s.bitpos("k", 1, 0, -1, false, false), Ok(10));
}

#[test]
fn bitpos_find_first_clear() {
   let s = store();
   for i in 0..8 {
      s.setbit("k", i, true).unwrap();
   }
   assert_eq!(s.bitpos("k", 0, 0, -1, false, false), Ok(8));
}

#[test]
fn bitpos_not_found() {
   let s = store();
   assert_eq!(s.bitpos("k", 1, 0, -1, false, false), Ok(-1));
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
