mod common;
use common::*;

#[test]
fn lpush_creates_list() {
   let s = store();
   assert_eq!(s.lpush("k", &["a", "b", "c"]), Ok(3));
   assert_eq!(s.llen("k"), Ok(3));
}

#[test]
fn rpush_creates_list() {
   let s = store();
   assert_eq!(s.rpush("k", &["a", "b", "c"]), Ok(3));
   assert_eq!(s.llen("k"), Ok(3));
}

#[test]
fn lpush_prepends_elements() {
   let s = store();
   s.rpush("k", &["a"]).unwrap();
   s.lpush("k", &["b", "c"]).unwrap();
   assert_eq!(
      s.lrange("k", 0, -1),
      Ok(vec!["c".into(), "b".into(), "a".into()])
   );
}

#[test]
fn rpush_appends_elements() {
   let s = store();
   s.rpush("k", &["a", "b", "c"]).unwrap();
   assert_eq!(
      s.lrange("k", 0, -1),
      Ok(vec!["a".into(), "b".into(), "c".into()])
   );
}

#[test]
fn lpop_single() {
   let s = store();
   s.rpush("k", &["a", "b", "c"]).unwrap();
   assert_eq!(s.lpop("k", 1), Ok(vec!["a".into()]));
   assert_eq!(s.llen("k"), Ok(2));
}

#[test]
fn rpop_single() {
   let s = store();
   s.rpush("k", &["a", "b", "c"]).unwrap();
   assert_eq!(s.rpop("k", 1), Ok(vec!["c".into()]));
   assert_eq!(s.llen("k"), Ok(2));
}

#[test]
fn lpop_multiple() {
   let s = store();
   s.rpush("k", &["a", "b", "c", "d"]).unwrap();
   assert_eq!(s.lpop("k", 2), Ok(vec!["a".into(), "b".into()]));
}

#[test]
fn rpop_multiple() {
   let s = store();
   s.rpush("k", &["a", "b", "c", "d"]).unwrap();
   assert_eq!(s.rpop("k", 2), Ok(vec!["d".into(), "c".into()]));
}

#[test]
fn lpop_empty_returns_empty() {
   let s = store();
   assert_eq!(s.lpop("k", 1), Ok(vec![]));
}

#[test]
fn rpop_empty_returns_empty() {
   let s = store();
   assert_eq!(s.rpop("k", 1), Ok(vec![]));
}

#[test]
fn llen_missing_key_returns_zero() {
   let s = store();
   assert_eq!(s.llen("k"), Ok(0));
}

#[test]
fn llen_wrong_type() {
   let s = store();
   set_str(&s, "k", "val");
   assert_eq!(s.llen("k"), Err("WRONGTYPE"));
}

#[test]
fn lindex_positive() {
   let s = store();
   s.rpush("k", &["a", "b", "c"]).unwrap();
   assert_eq!(s.lindex("k", 0), Ok(Some("a".into())));
   assert_eq!(s.lindex("k", 2), Ok(Some("c".into())));
}

#[test]
fn lindex_negative() {
   let s = store();
   s.rpush("k", &["a", "b", "c"]).unwrap();
   assert_eq!(s.lindex("k", -1), Ok(Some("c".into())));
   assert_eq!(s.lindex("k", -3), Ok(Some("a".into())));
}

#[test]
fn lindex_out_of_bounds() {
   let s = store();
   s.rpush("k", &["a", "b"]).unwrap();
   assert_eq!(s.lindex("k", 5), Ok(None));
   assert_eq!(s.lindex("k", -5), Ok(None));
}

#[test]
fn lset_updates_element() {
   let s = store();
   s.rpush("k", &["a", "b", "c"]).unwrap();
   assert_eq!(s.lset("k", 1, "x"), Ok(true));
   assert_eq!(s.lindex("k", 1), Ok(Some("x".into())));
}

#[test]
fn lset_out_of_range() {
   let s = store();
   s.rpush("k", &["a"]).unwrap();
   assert_eq!(s.lset("k", 5, "x"), Err("index out of range"));
}

#[test]
fn lset_missing_key() {
   let s = store();
   assert_eq!(s.lset("k", 0, "x"), Err("no such key"));
}

#[test]
fn lrange_full() {
   let s = store();
   s.rpush("k", &["a", "b", "c", "d"]).unwrap();
   assert_eq!(
      s.lrange("k", 0, -1),
      Ok(vec!["a".into(), "b".into(), "c".into(), "d".into()])
   );
}

#[test]
fn lrange_subset() {
   let s = store();
   s.rpush("k", &["a", "b", "c", "d"]).unwrap();
   assert_eq!(s.lrange("k", 1, 2), Ok(vec!["b".into(), "c".into()]));
}

#[test]
fn lrange_empty_on_missing_key() {
   let s = store();
   assert_eq!(s.lrange("k", 0, -1), Ok(vec![]));
}

#[test]
fn ltrim_keeps_range() {
   let s = store();
   s.rpush("k", &["a", "b", "c", "d", "e"]).unwrap();
   s.ltrim("k", 1, 3).unwrap();
   assert_eq!(
      s.lrange("k", 0, -1),
      Ok(vec!["b".into(), "c".into(), "d".into()])
   );
}

#[test]
fn ltrim_clears_on_invalid_range() {
   let s = store();
   s.rpush("k", &["a", "b"]).unwrap();
   s.ltrim("k", 5, 10).unwrap();
   assert_eq!(s.llen("k"), Ok(0));
}

#[test]
fn lrem_from_head() {
   let s = store();
   s.rpush("k", &["a", "b", "a", "c", "a"]).unwrap();
   assert_eq!(s.lrem("k", 2, "a"), Ok(2));
   assert_eq!(
      s.lrange("k", 0, -1),
      Ok(vec!["b".into(), "c".into(), "a".into()])
   );
}

#[test]
fn lrem_from_tail() {
   let s = store();
   s.rpush("k", &["a", "b", "a", "c", "a"]).unwrap();
   assert_eq!(s.lrem("k", -2, "a"), Ok(2));
   assert_eq!(
      s.lrange("k", 0, -1),
      Ok(vec!["a".into(), "b".into(), "c".into()])
   );
}

#[test]
fn lrem_all() {
   let s = store();
   s.rpush("k", &["a", "b", "a", "c", "a"]).unwrap();
   assert_eq!(s.lrem("k", 0, "a"), Ok(3));
   assert_eq!(s.lrange("k", 0, -1), Ok(vec!["b".into(), "c".into()]));
}

#[test]
fn linsert_before() {
   let s = store();
   s.rpush("k", &["a", "b", "c"]).unwrap();
   assert_eq!(s.linsert("k", true, "b", "x"), Ok(4));
   assert_eq!(
      s.lrange("k", 0, -1),
      Ok(vec!["a".into(), "x".into(), "b".into(), "c".into()])
   );
}

#[test]
fn linsert_after() {
   let s = store();
   s.rpush("k", &["a", "b", "c"]).unwrap();
   assert_eq!(s.linsert("k", false, "b", "x"), Ok(4));
   assert_eq!(
      s.lrange("k", 0, -1),
      Ok(vec!["a".into(), "b".into(), "x".into(), "c".into()])
   );
}

#[test]
fn linsert_pivot_not_found() {
   let s = store();
   s.rpush("k", &["a", "b"]).unwrap();
   assert_eq!(s.linsert("k", true, "z", "x"), Ok(-1));
}

#[test]
fn lpos_finds_element() {
   let s = store();
   s.rpush("k", &["a", "b", "c", "b", "d"]).unwrap();
   assert_eq!(s.lpos("k", "b", 1, 1, 0), Ok(vec![1]));
}

#[test]
fn lpos_multiple_matches() {
   let s = store();
   s.rpush("k", &["a", "b", "c", "b", "d", "b"]).unwrap();
   assert_eq!(s.lpos("k", "b", 1, 0, 0), Ok(vec![1, 3, 5]));
}

#[test]
fn lpos_not_found() {
   let s = store();
   s.rpush("k", &["a", "b"]).unwrap();
   assert_eq!(s.lpos("k", "z", 1, 1, 0), Ok(vec![]));
}

#[test]
fn lmove_left_to_right() {
   let s = store();
   s.rpush("src", &["a", "b", "c"]).unwrap();
   assert_eq!(s.lmove("src", "dst", true, false), Ok(Some("a".into())));
   assert_eq!(s.lrange("src", 0, -1), Ok(vec!["b".into(), "c".into()]));
   assert_eq!(s.lrange("dst", 0, -1), Ok(vec!["a".into()]));
}

#[test]
fn lmove_right_to_left() {
   let s = store();
   s.rpush("src", &["a", "b", "c"]).unwrap();
   assert_eq!(s.lmove("src", "dst", false, true), Ok(Some("c".into())));
   assert_eq!(s.lrange("src", 0, -1), Ok(vec!["a".into(), "b".into()]));
   assert_eq!(s.lrange("dst", 0, -1), Ok(vec!["c".into()]));
}

#[test]
fn lmove_empty_source_returns_none() {
   let s = store();
   assert_eq!(s.lmove("src", "dst", true, true), Ok(None));
}

#[test]
fn lpush_rpush_wrong_type() {
   let s = store();
   set_str(&s, "k", "val");
   assert_eq!(s.lpush("k", &["a"]), Err("WRONGTYPE"));
   assert_eq!(s.rpush("k", &["a"]), Err("WRONGTYPE"));
}

#[test]
fn lrange_to_buf_always_replies_on_empty_ranges() {
   let s = common::store();
   s.rpush("q", &["a"]).unwrap();
   let mut out = Vec::new();
   // start > stop used to write nothing at all (client hangs).
   s.lrange_to_buf("q", 1, 0, &mut out).unwrap();
   assert_eq!(out, b"*0\r\n");
   out.clear();
   let mut scratch = String::new();
   assert!(s.pop_one_to_scratch("q", false, &mut scratch).unwrap());
   // A drained list stays allocated; `0 -1` must not claim an element.
   s.lrange_to_buf("q", 0, -1, &mut out).unwrap();
   assert_eq!(out, b"*0\r\n");
   assert!(!s.exists("q"));
   assert_eq!(s.type_of("q"), "none");
}
