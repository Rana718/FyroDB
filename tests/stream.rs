mod common;
use common::*;
use fyro_db::storage::stream::{Trim, TrimBy};

#[test]
fn xadd_creates_stream() {
   let s = store();
   let fields = vec![("name".to_string(), "rana".to_string())];
   let id = s.xadd("k", "*", &fields, None, false).unwrap().unwrap();
   assert!(id.contains('-'));
   assert_eq!(s.xlen("k"), Ok(1));
}

#[test]
fn xadd_multiple_entries() {
   let s = store();
   for i in 0..5 {
      let fields = vec![("i".to_string(), i.to_string())];
      s.xadd("k", "*", &fields, None, false).unwrap();
   }
   assert_eq!(s.xlen("k"), Ok(5));
}

#[test]
fn xadd_with_explicit_id() {
   let s = store();
   let fields = vec![("a".to_string(), "1".to_string())];
   let id = s.xadd("k", "100-0", &fields, None, false).unwrap().unwrap();
   assert_eq!(id, "100-0");
}

#[test]
fn xadd_nomkstream_on_missing_key() {
   let s = store();
   let fields = vec![("a".to_string(), "1".to_string())];
   let result = s.xadd("k", "*", &fields, None, true).unwrap();
   assert_eq!(result, None);
   assert_eq!(s.xlen("k"), Ok(0));
}

#[test]
fn xadd_with_maxlen() {
   let s = store();
   for i in 0..10 {
      let fields = vec![("i".to_string(), i.to_string())];
      s.xadd(
         "k",
         "*",
         &fields,
         Some(Trim {
            by: TrimBy::MaxLen(5),
            approx: false,
            limit: 0,
         }),
         false,
      )
      .unwrap();
   }
   assert_eq!(s.xlen("k"), Ok(5));
}

#[test]
fn xlen_missing_key() {
   let s = store();
   assert_eq!(s.xlen("k"), Ok(0));
}

#[test]
fn xrange_full() {
   let s = store();
   for i in 1..=3 {
      let fields = vec![("v".to_string(), i.to_string())];
      s.xadd("k", &format!("{}-0", i), &fields, None, false)
         .unwrap();
   }
   let entries = s.xrange("k", "-", "+", 0).unwrap();
   assert_eq!(entries.len(), 3);
   assert_eq!(entries[0].0, "1-0");
   assert_eq!(entries[2].0, "3-0");
}

#[test]
fn xrange_with_count() {
   let s = store();
   for i in 1..=5 {
      let fields = vec![("v".to_string(), i.to_string())];
      s.xadd("k", &format!("{}-0", i), &fields, None, false)
         .unwrap();
   }
   let entries = s.xrange("k", "-", "+", 2).unwrap();
   assert_eq!(entries.len(), 2);
}

#[test]
fn xrange_subset() {
   let s = store();
   for i in 1..=5 {
      let fields = vec![("v".to_string(), i.to_string())];
      s.xadd("k", &format!("{}-0", i), &fields, None, false)
         .unwrap();
   }
   let entries = s.xrange("k", "2-0", "4-0", 0).unwrap();
   assert_eq!(entries.len(), 3);
}

#[test]
fn xrevrange_full() {
   let s = store();
   for i in 1..=3 {
      let fields = vec![("v".to_string(), i.to_string())];
      s.xadd("k", &format!("{}-0", i), &fields, None, false)
         .unwrap();
   }
   let entries = s.xrevrange("k", "+", "-", 0).unwrap();
   assert_eq!(entries.len(), 3);
   assert_eq!(entries[0].0, "3-0");
   assert_eq!(entries[2].0, "1-0");
}

#[test]
fn xtrim_reduces_length() {
   let s = store();
   for i in 1..=10 {
      let fields = vec![("v".to_string(), i.to_string())];
      s.xadd("k", &format!("{}-0", i), &fields, None, false)
         .unwrap();
   }
   let removed = s
      .xtrim(
         "k",
         Trim {
            by: TrimBy::MaxLen(5),
            approx: false,
            limit: 0,
         },
      )
      .unwrap();
   assert_eq!(removed, 5);
   assert_eq!(s.xlen("k"), Ok(5));
}

#[test]
fn xdel_removes_entries() {
   let s = store();
   for i in 1..=3 {
      let fields = vec![("v".to_string(), i.to_string())];
      s.xadd("k", &format!("{}-0", i), &fields, None, false)
         .unwrap();
   }
   let removed = s.xdel("k", &["2-0"]).unwrap();
   assert_eq!(removed, 1);
   assert_eq!(s.xlen("k"), Ok(2));
}

#[test]
fn xdel_missing_id() {
   let s = store();
   let fields = vec![("v".to_string(), "1".to_string())];
   s.xadd("k", "1-0", &fields, None, false).unwrap();
   assert_eq!(s.xdel("k", &["99-0"]), Ok(0));
}

#[test]
fn xgroup_create_and_destroy() {
   let s = store();
   let fields = vec![("v".to_string(), "1".to_string())];
   s.xadd("k", "*", &fields, None, false).unwrap();
   assert_eq!(s.xgroup_create("k", "mygroup", "$", false), Ok(true));
   assert_eq!(s.xgroup_destroy("k", "mygroup"), Ok(true));
   assert_eq!(s.xgroup_destroy("k", "mygroup"), Ok(false));
}

#[test]
fn xgroup_create_mkstream() {
   let s = store();
   assert_eq!(s.xgroup_create("k", "g", "0", true), Ok(true));
   assert_eq!(s.xlen("k"), Ok(0));
}

#[test]
fn xgroup_create_missing_key_no_mkstream() {
   let s = store();
   assert_eq!(
      s.xgroup_create("k", "g", "0", false),
      Err(fyro_db::storage::stream::ERR_NO_KEY_GROUP)
   );
}

#[test]
fn xack_acknowledges_entries() {
   let s = store();
   let fields = vec![("v".to_string(), "1".to_string())];
   s.xadd("k", "1-0", &fields, None, false).unwrap();
   s.xgroup_create("k", "g", "0", false).unwrap();
   assert_eq!(s.xack("k", "g", &["1-0"]), Ok(0));
}

#[test]
fn xlen_wrong_type() {
   let s = store();
   set_str(&s, "k", "val");
   assert_eq!(s.xlen("k"), Err("WRONGTYPE"));
}

#[test]
fn xadd_wrong_type() {
   let s = store();
   set_str(&s, "k", "val");
   let fields = vec![("a".to_string(), "1".to_string())];
   assert_eq!(s.xadd("k", "*", &fields, None, false), Err("WRONGTYPE"));
}
