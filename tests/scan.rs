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

#[test]
fn test_hscan_sscan_zscan_framing() {
   let s = store();
   let mut out = Vec::new();

   // Empty key scans must produce cursor "0" and empty array "*2\r\n$1\r\n0\r\n*0\r\n"
   fyro_db::commands::execute(&["HSCAN", "empty_hash", "0"], &s, &mut out);
   assert_eq!(out, b"*2\r\n$1\r\n0\r\n*0\r\n");

   out.clear();
   fyro_db::commands::execute(&["SSCAN", "empty_set", "0"], &s, &mut out);
   assert_eq!(out, b"*2\r\n$1\r\n0\r\n*0\r\n");

   out.clear();
   fyro_db::commands::execute(&["ZSCAN", "empty_zset", "0"], &s, &mut out);
   assert_eq!(out, b"*2\r\n$1\r\n0\r\n*0\r\n");

   // Populated data
   s.hset("h1", &[("f1", "v1"), ("f2", "v2")]).unwrap();
   out.clear();
   fyro_db::commands::execute(&["HSCAN", "h1", "0"], &s, &mut out);
   assert!(out.starts_with(b"*2\r\n$1\r\n0\r\n*4\r\n"));

   s.sadd("s1", &["item1", "item2"]).unwrap();
   out.clear();
   fyro_db::commands::execute(&["SSCAN", "s1", "0"], &s, &mut out);
   assert!(out.starts_with(b"*2\r\n$1\r\n0\r\n*2\r\n"));

   out.clear();
   fyro_db::commands::execute(&["ZADD", "z1", "10", "m1", "20", "m2"], &s, &mut out);
   out.clear();
   fyro_db::commands::execute(&["ZSCAN", "z1", "0"], &s, &mut out);
   assert!(out.starts_with(b"*2\r\n$1\r\n0\r\n*4\r\n"));
}

#[test]
fn test_hscan_sscan_zscan_match_pattern() {
   let s = store();
   let mut out = Vec::new();

   // HSCAN with MATCH
   s.hset(
      "user:profile",
      &[("name", "Alice"), ("age", "30"), ("nickname", "Ali")],
   )
   .unwrap();
   fyro_db::commands::execute(&["HSCAN", "user:profile", "0", "MATCH", "n*"], &s, &mut out);
   let resp = String::from_utf8_lossy(&out);
   assert!(resp.contains("name"));
   assert!(resp.contains("nickname"));
   assert!(!resp.contains("age"));

   // SSCAN with MATCH
   out.clear();
   s.sadd("tags", &["rust", "ruby", "python", "golang"])
      .unwrap();
   fyro_db::commands::execute(&["SSCAN", "tags", "0", "MATCH", "r*"], &s, &mut out);
   let resp = String::from_utf8_lossy(&out);
   assert!(resp.contains("rust"));
   assert!(resp.contains("ruby"));
   assert!(!resp.contains("python"));
   assert!(!resp.contains("golang"));

   // ZSCAN with MATCH
   out.clear();
   fyro_db::commands::execute(
      &[
         "ZADD",
         "leaderboard",
         "100",
         "team_alpha",
         "200",
         "team_beta",
         "300",
         "solo_gamma",
      ],
      &s,
      &mut out,
   );
   out.clear();
   fyro_db::commands::execute(
      &["ZSCAN", "leaderboard", "0", "MATCH", "team_*"],
      &s,
      &mut out,
   );
   let resp = String::from_utf8_lossy(&out);
   assert!(resp.contains("team_alpha"));
   assert!(resp.contains("team_beta"));
   assert!(!resp.contains("solo_gamma"));
}

#[test]
fn test_scan_expired_keys_skipped() {
   let s = store();
   // Insert live key and expired key (deadline in past)
   s.set_string("live_key", "valid", 0);
   s.set_string("expired_key", "stale", 1); // expired long ago

   let (_, keys) = s.scan(0, None, 100);
   assert!(keys.contains(&"live_key".to_string()));
   assert!(!keys.contains(&"expired_key".to_string()));
}
