mod common;
use common::*;
use std::time::Duration;

// DEL

#[test]
fn del_existing_key() {
   let s = store();
   set_str(&s, "k", "v");
   assert!(s.del("k"));
   assert_eq!(s.get("k"), None);
}

#[test]
fn del_missing_key_returns_false() {
   let s = store();
   assert!(!s.del("nope"));
}

// EXISTS

#[test]
fn exists_true_for_live_key() {
   let s = store();
   set_str(&s, "k", "v");
   assert!(s.exists("k"));
}

#[test]
fn exists_false_for_missing_key() {
   let s = store();
   assert!(!s.exists("nope"));
}

#[test]
fn exists_false_for_expired_key() {
   let s = store();
   set_expired(&s, "k", "v");
   std::thread::sleep(Duration::from_millis(5));
   assert!(!s.exists("k"));
}

// EXPIRE / TTL / PERSIST

#[test]
fn expire_sets_ttl() {
   let s = store();
   set_str(&s, "k", "v");
   assert!(s.expire("k", Duration::from_secs(100)));
   let ttl = s.ttl("k").unwrap();
   assert!(ttl.as_secs() > 90 && ttl.as_secs() <= 100);
}

#[test]
fn expire_returns_false_on_missing_key() {
   let s = store();
   assert!(!s.expire("nope", Duration::from_secs(10)));
}

#[test]
fn ttl_none_for_persistent_key() {
   let s = store();
   set_str(&s, "k", "v");
   assert_eq!(s.ttl("k"), None);
}

#[test]
fn pttl_returns_millis() {
   let s = store();
   set_str(&s, "k", "v");
   s.expire("k", Duration::from_secs(10));
   let pttl = s.pttl("k").unwrap();
   assert!(pttl.as_millis() > 9000 && pttl.as_millis() <= 10000);
}

#[test]
fn persist_removes_expiry() {
   let s = store();
   set_str(&s, "k", "v");
   s.expire("k", Duration::from_secs(100));
   assert!(s.persist("k"));
   assert_eq!(s.ttl("k"), None);
}

#[test]
fn persist_returns_false_when_no_expiry() {
   let s = store();
   set_str(&s, "k", "v");
   assert!(!s.persist("k")); // already persistent
}

// RENAME / RENAMENX

#[test]
fn rename_moves_key() {
   let s = store();
   set_str(&s, "old", "v");
   assert!(s.rename("old", "new"));
   assert_eq!(s.get("new"), Some("v".into()));
   assert_eq!(s.get("old"), None);
}

#[test]
fn rename_missing_key_returns_false() {
   let s = store();
   assert!(!s.rename("nope", "new"));
}

#[test]
fn renamenx_only_renames_if_dst_absent() {
   let s = store();
   set_str(&s, "src", "v");
   set_str(&s, "dst", "existing");
   assert!(!s.renamenx("src", "dst"));
   assert!(s.renamenx("src", "new"));
}

// COPY

#[test]
fn copy_to_new_key() {
   let s = store();
   set_str(&s, "src", "v");
   assert!(s.copy("src", "dst", false));
   assert_eq!(s.get("dst"), Some("v".into()));
   assert_eq!(s.get("src"), Some("v".into()));
}

#[test]
fn copy_no_replace_when_dst_exists() {
   let s = store();
   set_str(&s, "src", "new");
   set_str(&s, "dst", "old");
   assert!(!s.copy("src", "dst", false));
   assert_eq!(s.get("dst"), Some("old".into()));
}

#[test]
fn copy_with_replace_overwrites() {
   let s = store();
   set_str(&s, "src", "new");
   set_str(&s, "dst", "old");
   assert!(s.copy("src", "dst", true));
   assert_eq!(s.get("dst"), Some("new".into()));
}

// RANDOMKEY

#[test]
fn randomkey_returns_some_when_keys_exist() {
   let s = store();
   set_str(&s, "a", "1");
   assert!(s.randomkey().is_some());
}

#[test]
fn randomkey_returns_none_on_empty_store() {
   let s = store();
   assert_eq!(s.randomkey(), None);
}

// KEYS_MATCHING

#[test]
fn keys_matching_wildcard() {
   let s = store();
   set_str(&s, "user:1", "a");
   set_str(&s, "user:2", "b");
   set_str(&s, "post:1", "c");
   let mut keys = s.keys_matching("user:*");
   keys.sort();
   assert_eq!(keys, vec!["user:1", "user:2"]);
}

#[test]
fn keys_matching_question_mark() {
   let s = store();
   set_str(&s, "key1", "a");
   set_str(&s, "key2", "b");
   set_str(&s, "key10", "c");
   let mut keys = s.keys_matching("key?");
   keys.sort();
   assert_eq!(keys, vec!["key1", "key2"]);
}

#[test]
fn keys_matching_all() {
   let s = store();
   set_str(&s, "a", "1");
   set_str(&s, "b", "2");
   assert_eq!(s.keys_matching("*").len(), 2);
}
