mod common;
use common::*;

// SCAN

#[test]
fn scan_empty_store() {
   let s = store();
   let (cursor, keys) = s.scan(0, None, 10);
   assert_eq!(cursor, 0);
   assert!(keys.is_empty());
}

#[test]
fn scan_returns_all_keys_cursor_zero_at_end() {
   let s = store();
   for i in 0..5 {
      set_str(&s, &format!("k{}", i), "v");
   }
   let (cursor, keys) = s.scan(0, None, 10);
   assert_eq!(cursor, 0);
   assert_eq!(keys.len(), 5);
}

#[test]
fn scan_paginates_with_count() {
   let s = store();
   for i in 0..10 {
      set_str(&s, &format!("k{:02}", i), "v");
   }

   let mut all = vec![];
   let mut cursor = 0;
   loop {
      let (next, keys) = s.scan(cursor, None, 3);
      all.extend(keys);
      cursor = next;
      if cursor == 0 {
         break;
      }
   }
   all.sort();
   assert_eq!(all.len(), 10);
   assert_eq!(all[0], "k00");
}

#[test]
fn scan_match_pattern() {
   let s = store();
   set_str(&s, "user:1", "a");
   set_str(&s, "user:2", "b");
   set_str(&s, "post:1", "c");

   // Use a large enough count to get all keys in one pass
   let (_, keys) = s.scan(0, Some("user:*"), 100);
   assert_eq!(keys.len(), 2);
   assert!(keys.iter().all(|k| k.starts_with("user:")));
}

#[test]
fn scan_returns_all_keys_unsorted() {
   let s = store();
   set_str(&s, "b", "2");
   set_str(&s, "a", "1");
   set_str(&s, "c", "3");
   let (_, mut keys) = s.scan(0, None, 10);
   keys.sort();
   assert_eq!(keys, vec!["a", "b", "c"]);
}
