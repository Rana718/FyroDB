mod common;
use common::*;
use foldhash::{HashMap, HashMapExt};
use fyro_db::storage::value::StoreValue;

// SET / GET

#[test]
fn set_and_get() {
   let s = store();
   set_str(&s, "k", "hello");
   assert_eq!(s.get("k"), Some("hello".into()));
}

#[test]
fn get_missing_key_returns_none() {
   let s = store();
   assert_eq!(s.get("nope"), None);
}

#[test]
fn get_expired_key_returns_none_and_removes() {
   let s = store();
   set_expired(&s, "k", "v");
   std::thread::sleep(std::time::Duration::from_millis(5));
   assert_eq!(s.get("k"), None);
   assert_eq!(s.dbsize(), 0);
}

#[test]
fn set_overwrites_existing_key() {
   let s = store();
   set_str(&s, "k", "old");
   set_str(&s, "k", "new");
   assert_eq!(s.get("k"), Some("new".into()));
}

// GETDEL

#[test]
fn getdel_returns_value_and_removes() {
   let s = store();
   set_str(&s, "k", "v");
   assert_eq!(s.getdel("k"), Some("v".into()));
   assert_eq!(s.get("k"), None);
}

#[test]
fn getdel_missing_returns_none() {
   let s = store();
   assert_eq!(s.getdel("nope"), None);
}

// GETSET

#[test]
fn getset_returns_old_sets_new() {
   let s = store();
   set_str(&s, "k", "old");
   assert_eq!(s.getset("k", "new"), Some("old".into()));
   assert_eq!(s.get("k"), Some("new".into()));
}

#[test]
fn getset_on_missing_returns_none() {
   let s = store();
   assert_eq!(s.getset("k", "v"), None);
   assert_eq!(s.get("k"), Some("v".into()));
}

// SETNX

#[test]
fn setnx_sets_when_absent() {
   let s = store();
   assert!(s.setnx("k".into(), "v".into()));
   assert_eq!(s.get("k"), Some("v".into()));
}

#[test]
fn setnx_does_not_overwrite() {
   let s = store();
   set_str(&s, "k", "original");
   assert!(!s.setnx("k".into(), "new".into()));
   assert_eq!(s.get("k"), Some("original".into()));
}

#[test]
fn concurrent_increments_do_not_lose_updates() {
   let s = store();
   let workers = 8;
   let increments = 1_000;
   let mut threads = Vec::new();
   for _ in 0..workers {
      let s = std::sync::Arc::clone(&s);
      threads.push(std::thread::spawn(move || {
         for _ in 0..increments {
            s.incr("counter").unwrap();
         }
      }));
   }
   for thread in threads {
      thread.join().unwrap();
   }
   assert_eq!(s.get("counter"), Some((workers * increments).to_string()));
}

#[test]
fn integer_overflow_returns_an_error() {
   let s = store();
   set_str(&s, "counter", &i64::MAX.to_string());
   assert_eq!(
      s.incr("counter"),
      Err("increment or decrement would overflow")
   );
   assert_eq!(s.get("counter"), Some(i64::MAX.to_string()));
}

// APPEND

#[test]
fn append_to_existing() {
   let s = store();
   set_str(&s, "k", "hello");
   assert_eq!(s.append("k", " world"), Ok(11));
   assert_eq!(s.get("k"), Some("hello world".into()));
}

#[test]
fn append_to_missing_creates_key() {
   let s = store();
   assert_eq!(s.append("k", "hi"), Ok(2));
   assert_eq!(s.get("k"), Some("hi".into()));
}

#[test]
fn append_wrong_type_errors() {
   let s = store();
   s.set("k".into(), StoreValue::hash(HashMap::new()));
   assert!(s.append("k", "x").is_err());
}

// STRLEN

#[test]
fn strlen_existing() {
   let s = store();
   set_str(&s, "k", "hello");
   assert_eq!(s.strlen("k"), Ok(5));
}

#[test]
fn strlen_missing_returns_zero() {
   let s = store();
   assert_eq!(s.strlen("nope"), Ok(0));
}

// GETRANGE

#[test]
fn getrange_positive_indices() {
   let s = store();
   set_str(&s, "k", "hello world");
   assert_eq!(s.getrange("k", 0, 4), "hello");
   assert_eq!(s.getrange("k", 6, 10), "world");
}

#[test]
fn getrange_negative_indices() {
   let s = store();
   set_str(&s, "k", "hello");
   assert_eq!(s.getrange("k", -3, -1), "llo");
}

#[test]
fn getrange_out_of_bounds_clamps() {
   let s = store();
   set_str(&s, "k", "hi");
   assert_eq!(s.getrange("k", 0, 100), "hi");
}

#[test]
fn getrange_missing_key_empty() {
   let s = store();
   assert_eq!(s.getrange("nope", 0, 5), "");
}

// SETRANGE

#[test]
fn setrange_overwrites_in_place() {
   let s = store();
   set_str(&s, "k", "hello world");
   assert_eq!(s.setrange("k", 6, "Redis"), Ok(11));
   assert_eq!(s.get("k"), Some("hello Redis".into()));
}

#[test]
fn setrange_pads_with_nulls_on_new_key() {
   let s = store();
   let len = s.setrange("k", 3, "hi").unwrap();
   assert_eq!(len, 5);
   let v = s.get("k").unwrap();
   assert_eq!(&v[3..], "hi");
}

// INCR / DECR / INCRBY / DECRBY

#[test]
fn incr_creates_key_at_one() {
   let s = store();
   assert_eq!(s.incr("k"), Ok(1));
}

#[test]
fn incr_increments_existing() {
   let s = store();
   set_str(&s, "k", "10");
   assert_eq!(s.incr("k"), Ok(11));
}

#[test]
fn decr_decrements() {
   let s = store();
   set_str(&s, "k", "5");
   assert_eq!(s.decr("k"), Ok(4));
}

#[test]
fn incrby_adds_delta() {
   let s = store();
   set_str(&s, "k", "10");
   assert_eq!(s.incrby("k", 5), Ok(15));
   assert_eq!(s.incrby("k", -3), Ok(12));
}

#[test]
fn decrby_subtracts_delta() {
   let s = store();
   set_str(&s, "k", "10");
   assert_eq!(s.decrby("k", 4), Ok(6));
}

#[test]
fn incr_errors_on_non_integer() {
   let s = store();
   set_str(&s, "k", "notanumber");
   assert!(s.incr("k").is_err());
}

#[test]
fn incr_errors_on_wrong_type() {
   let s = store();
   s.set("k".into(), StoreValue::hash(HashMap::new()));
   assert!(s.incr("k").is_err());
}

// INCRBYFLOAT

#[test]
fn incrbyfloat_basic() {
   let s = store();
   set_str(&s, "k", "10.5");
   let r = s.incrbyfloat("k", 0.1).unwrap();
   assert!((r - 10.6).abs() < 1e-9);
}

#[test]
fn incrbyfloat_creates_key() {
   let s = store();
   let r = s.incrbyfloat("k", 3.125).unwrap();
   assert!((r - 3.125).abs() < 1e-9);
}

#[test]
fn incrbyfloat_errors_on_non_float() {
   let s = store();
   set_str(&s, "k", "abc");
   assert!(s.incrbyfloat("k", 1.0).is_err());
}
