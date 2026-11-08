mod common;
use common::*;
use fyro_db::storage::zset::{ZAddOptions, ZAggregate};

fn zadd_simple(s: &fyro_db::storage::store::Store, key: &str, members: &[(f64, &str)]) {
   s.zadd(key, members, ZAddOptions::default()).unwrap();
}

#[test]
fn zadd_creates_zset() {
   let s = store();
   let members = vec![(1.0, "a"), (2.0, "b"), (3.0, "c")];
   assert_eq!(s.zadd("k", &members, ZAddOptions::default()), Ok(3));
   assert_eq!(s.zcard("k"), Ok(3));
}

#[test]
fn zadd_missing_key_counts_unique_new_members() {
   let s = store();
   let members = [(1.0, "a"), (2.0, "b"), (3.0, "a")];
   assert_eq!(s.zadd("k", &members, ZAddOptions::default()), Ok(2));
   assert_eq!(s.zcard("k"), Ok(2));
   assert_eq!(s.zscore("k", "a"), Ok(Some(3.0)));
   assert_eq!(s.zscore("k", "b"), Ok(Some(2.0)));
}

#[test]
fn zadd_existing_key_processes_duplicate_members_in_order() {
   let s = store();
   zadd_simple(&s, "k", &[(1.0, "a")]);
   let members = [(2.0, "a"), (3.0, "b"), (4.0, "b")];
   assert_eq!(s.zadd("k", &members, ZAddOptions::default()), Ok(1));
   assert_eq!(s.zcard("k"), Ok(2));
   assert_eq!(s.zscore("k", "a"), Ok(Some(2.0)));
   assert_eq!(s.zscore("k", "b"), Ok(Some(4.0)));
}

#[test]
fn zadd_ch_counts_only_actual_changes() {
   let s = store();
   zadd_simple(&s, "k", &[(1.0, "a"), (2.0, "b")]);
   let ch = ZAddOptions {
      ch: true,
      ..Default::default()
   };
   assert_eq!(
      s.zadd("k", &[(1.0, "a"), (3.0, "b"), (4.0, "c")], ch),
      Ok(2)
   );
   assert_eq!(s.zcard("k"), Ok(3));
   assert_eq!(s.zscore("k", "a"), Ok(Some(1.0)));
   assert_eq!(s.zscore("k", "b"), Ok(Some(3.0)));
   assert_eq!(s.zscore("k", "c"), Ok(Some(4.0)));
}

#[test]
fn zadd_ch_counts_conditional_updates_and_additions() {
   let s = store();
   zadd_simple(&s, "k", &[(2.0, "a"), (2.0, "b")]);
   let gt_ch = ZAddOptions {
      gt: true,
      ch: true,
      ..Default::default()
   };
   assert_eq!(
      s.zadd("k", &[(3.0, "a"), (1.0, "b"), (4.0, "c")], gt_ch),
      Ok(2)
   );
   assert_eq!(s.zscore("k", "a"), Ok(Some(3.0)));
   assert_eq!(s.zscore("k", "b"), Ok(Some(2.0)));
   assert_eq!(s.zscore("k", "c"), Ok(Some(4.0)));

   let xx_ch = ZAddOptions {
      xx: true,
      ch: true,
      ..Default::default()
   };
   assert_eq!(
      s.zadd("k", &[(3.0, "a"), (5.0, "b"), (6.0, "d")], xx_ch),
      Ok(1)
   );
   assert_eq!(s.zcard("k"), Ok(3));
   assert_eq!(s.zscore("k", "b"), Ok(Some(5.0)));
   assert_eq!(s.zscore("k", "d"), Ok(None));
}

#[test]
fn zadd_xx_does_not_create_missing_or_expired_key() {
   let s = store();
   let xx = ZAddOptions {
      xx: true,
      ..Default::default()
   };
   assert_eq!(s.zadd("missing", &[(1.0, "a")], xx), Ok(0));
   assert!(!s.exists("missing"));

   zadd_simple(&s, "expired", &[(1.0, "a")]);
   assert!(s.expire_ms("expired", 1));
   assert_eq!(s.zadd("expired", &[(2.0, "a")], xx), Ok(0));
   assert!(!s.exists("expired"));
   assert_eq!(s.zscore("expired", "a"), Ok(None));
}

#[test]
fn zadd_replaces_expired_key_and_clears_expiry() {
   let s = store();
   zadd_simple(&s, "k", &[(9.0, "old")]);
   assert!(s.expire_ms("k", 1));
   assert_eq!(
      s.zadd("k", &[(1.0, "a"), (2.0, "a")], ZAddOptions::default()),
      Ok(1)
   );
   assert!(s.exists("k"));
   assert_eq!(s.ttl("k"), None);
   assert_eq!(s.zcard("k"), Ok(1));
   assert_eq!(s.zscore("k", "old"), Ok(None));
   assert_eq!(s.zscore("k", "a"), Ok(Some(2.0)));
}

#[test]
fn zadd_wrong_type_preserves_value() {
   let s = store();
   set_str(&s, "k", "value");
   assert_eq!(
      s.zadd("k", &[(1.0, "a")], ZAddOptions::default()),
      Err("WRONGTYPE")
   );
   assert_eq!(s.get("k"), Some("value".into()));
}

#[test]
fn zadd_updates_score() {
   let s = store();
   zadd_simple(&s, "k", &[(1.0, "a")]);
   zadd_simple(&s, "k", &[(5.0, "a")]);
   assert_eq!(s.zscore("k", "a"), Ok(Some(5.0)));
   assert_eq!(s.zcard("k"), Ok(1));
}

#[test]
fn zadd_nx_does_not_update() {
   let s = store();
   zadd_simple(&s, "k", &[(1.0, "a")]);
   let m = vec![(5.0, "a")];
   s.zadd(
      "k",
      &m,
      ZAddOptions {
         nx: true,
         ..Default::default()
      },
   )
   .unwrap();
   assert_eq!(s.zscore("k", "a"), Ok(Some(1.0)));
}

#[test]
fn zadd_xx_only_updates_existing() {
   let s = store();
   zadd_simple(&s, "k", &[(1.0, "a")]);
   let m = vec![(5.0, "a"), (3.0, "b")];
   let added = s
      .zadd(
         "k",
         &m,
         ZAddOptions {
            xx: true,
            ..Default::default()
         },
      )
      .unwrap();
   assert_eq!(added, 0);
   assert_eq!(s.zscore("k", "a"), Ok(Some(5.0)));
   assert_eq!(s.zscore("k", "b"), Ok(None));
}

#[test]
fn zadd_gt_only_increases() {
   let s = store();
   zadd_simple(&s, "k", &[(5.0, "a")]);
   let m = vec![(3.0, "a")];
   s.zadd(
      "k",
      &m,
      ZAddOptions {
         gt: true,
         ..Default::default()
      },
   )
   .unwrap();
   assert_eq!(s.zscore("k", "a"), Ok(Some(5.0)));
   let m2 = vec![(10.0, "a")];
   s.zadd(
      "k",
      &m2,
      ZAddOptions {
         gt: true,
         ..Default::default()
      },
   )
   .unwrap();
   assert_eq!(s.zscore("k", "a"), Ok(Some(10.0)));
}

#[test]
fn zadd_lt_only_decreases() {
   let s = store();
   zadd_simple(&s, "k", &[(5.0, "a")]);
   let m = vec![(10.0, "a")];
   s.zadd(
      "k",
      &m,
      ZAddOptions {
         lt: true,
         ..Default::default()
      },
   )
   .unwrap();
   assert_eq!(s.zscore("k", "a"), Ok(Some(5.0)));
   let m2 = vec![(2.0, "a")];
   s.zadd(
      "k",
      &m2,
      ZAddOptions {
         lt: true,
         ..Default::default()
      },
   )
   .unwrap();
   assert_eq!(s.zscore("k", "a"), Ok(Some(2.0)));
}

#[test]
fn zrem_removes_members() {
   let s = store();
   zadd_simple(&s, "k", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]);
   assert_eq!(s.zrem("k", &["a", "c"]), Ok(2));
   assert_eq!(s.zcard("k"), Ok(1));
}

#[test]
fn zrem_missing_returns_zero() {
   let s = store();
   zadd_simple(&s, "k", &[(1.0, "a")]);
   assert_eq!(s.zrem("k", &["z"]), Ok(0));
}

#[test]
fn zscore_existing() {
   let s = store();
   zadd_simple(&s, "k", &[(std::f64::consts::PI, "pi")]);
   let score = s.zscore("k", "pi").unwrap().unwrap();
   assert!((score - std::f64::consts::PI).abs() < 0.001);
}

#[test]
fn zscore_missing_member() {
   let s = store();
   zadd_simple(&s, "k", &[(1.0, "a")]);
   assert_eq!(s.zscore("k", "z"), Ok(None));
}

#[test]
fn zscore_missing_key() {
   let s = store();
   assert_eq!(s.zscore("k", "a"), Ok(None));
}

#[test]
fn zmscore_mixed() {
   let s = store();
   zadd_simple(&s, "k", &[(1.0, "a"), (2.0, "b")]);
   let scores = s.zmscore("k", &["a", "z", "b"]).unwrap();
   assert_eq!(scores[0], Some(1.0));
   assert_eq!(scores[1], None);
   assert_eq!(scores[2], Some(2.0));
}

#[test]
fn zmscore_to_buf_matches_zmscore() {
   let s = store();
   zadd_simple(&s, "k", &[(1.5, "a"), (2.5, "b")]);
   let mut out = Vec::new();
   s.zmscore_to_buf("k", &["a", "z", "b"], &mut out).unwrap();
   assert_eq!(out, b"*3\r\n$3\r\n1.5\r\n$-1\r\n$3\r\n2.5\r\n");

   let mut out = Vec::new();
   s.zmscore_to_buf("nope", &["a"], &mut out).unwrap();
   assert_eq!(out, b"*1\r\n$-1\r\n");
}

#[test]
fn zmscore_to_buf_wrongtype_errors_without_leaking_bytes() {
   let s = store();
   s.set_string("strk", "x", 0);
   let mut out = Vec::new();
   assert_eq!(s.zmscore_to_buf("strk", &["a"], &mut out), Err("WRONGTYPE"));
   assert!(out.is_empty());
}

#[test]
fn zrank_ascending() {
   let s = store();
   zadd_simple(&s, "k", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]);
   assert_eq!(s.zrank("k", "a"), Ok(Some(0)));
   assert_eq!(s.zrank("k", "b"), Ok(Some(1)));
   assert_eq!(s.zrank("k", "c"), Ok(Some(2)));
}

#[test]
fn zrank_missing() {
   let s = store();
   zadd_simple(&s, "k", &[(1.0, "a")]);
   assert_eq!(s.zrank("k", "z"), Ok(None));
}

#[test]
fn zrevrank_descending() {
   let s = store();
   zadd_simple(&s, "k", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]);
   assert_eq!(s.zrevrank("k", "c"), Ok(Some(0)));
   assert_eq!(s.zrevrank("k", "a"), Ok(Some(2)));
}

#[test]
fn zcard_empty() {
   let s = store();
   assert_eq!(s.zcard("k"), Ok(0));
}

#[test]
fn zcount_range() {
   let s = store();
   zadd_simple(&s, "k", &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d")]);
   assert_eq!(s.zcount("k", 2.0, 3.0), Ok(2));
}

#[test]
fn zcount_all() {
   let s = store();
   zadd_simple(&s, "k", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]);
   assert_eq!(s.zcount("k", f64::NEG_INFINITY, f64::INFINITY), Ok(3));
}

#[test]
fn zincrby_existing() {
   let s = store();
   zadd_simple(&s, "k", &[(5.0, "a")]);
   assert_eq!(s.zincrby("k", 3.0, "a"), Ok(8.0));
}

#[test]
fn zincrby_creates_member() {
   let s = store();
   assert_eq!(s.zincrby("k", 7.0, "a"), Ok(7.0));
   assert_eq!(s.zscore("k", "a"), Ok(Some(7.0)));
}

#[test]
fn zrange_by_rank() {
   let s = store();
   zadd_simple(&s, "k", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]);
   let items = s.zrange("k", 0, -1, false).unwrap();
   let members: Vec<&str> = items.iter().map(|(m, _)| m.as_str()).collect();
   assert_eq!(members, vec!["a", "b", "c"]);
}

#[test]
fn zrange_subset() {
   let s = store();
   zadd_simple(&s, "k", &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d")]);
   let items = s.zrange("k", 1, 2, false).unwrap();
   let members: Vec<&str> = items.iter().map(|(m, _)| m.as_str()).collect();
   assert_eq!(members, vec!["b", "c"]);
}

#[test]
fn zrevrange_by_rank() {
   let s = store();
   zadd_simple(&s, "k", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]);
   let items = s.zrevrange("k", 0, -1).unwrap();
   let members: Vec<&str> = items.iter().map(|(m, _)| m.as_str()).collect();
   assert_eq!(members, vec!["c", "b", "a"]);
}

#[test]
fn zrangebyscore_range() {
   let s = store();
   zadd_simple(&s, "k", &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d")]);
   let items = s.zrangebyscore("k", 2.0, 3.0, 0, 0).unwrap();
   let members: Vec<&str> = items.iter().map(|(m, _)| m.as_str()).collect();
   assert_eq!(members, vec!["b", "c"]);
}

#[test]
fn zrangebyscore_with_limit() {
   let s = store();
   zadd_simple(&s, "k", &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d")]);
   let items = s.zrangebyscore("k", 1.0, 4.0, 1, 2).unwrap();
   let members: Vec<&str> = items.iter().map(|(m, _)| m.as_str()).collect();
   assert_eq!(members, vec!["b", "c"]);
}

#[test]
fn zrevrangebyscore_range() {
   let s = store();
   zadd_simple(&s, "k", &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d")]);
   let items = s.zrevrangebyscore("k", 3.0, 1.0, 0, 0).unwrap();
   let members: Vec<&str> = items.iter().map(|(m, _)| m.as_str()).collect();
   assert_eq!(members, vec!["c", "b", "a"]);
}

#[test]
fn zpopmin_pops_lowest() {
   let s = store();
   zadd_simple(&s, "k", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]);
   let items = s.zpopmin("k", 2).unwrap();
   assert_eq!(items.len(), 2);
   assert_eq!(items[0].0, "a");
   assert_eq!(items[1].0, "b");
   assert_eq!(s.zcard("k"), Ok(1));
}

#[test]
fn zpopmax_pops_highest() {
   let s = store();
   zadd_simple(&s, "k", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]);
   let items = s.zpopmax("k", 2).unwrap();
   assert_eq!(items.len(), 2);
   assert_eq!(items[0].0, "c");
   assert_eq!(items[1].0, "b");
   assert_eq!(s.zcard("k"), Ok(1));
}

#[test]
fn zpopmin_empty_returns_empty() {
   let s = store();
   assert_eq!(s.zpopmin("k", 1), Ok(vec![]));
}

#[test]
fn zunionstore_combines() {
   let s = store();
   zadd_simple(&s, "a", &[(1.0, "x"), (2.0, "y")]);
   zadd_simple(&s, "b", &[(3.0, "y"), (4.0, "z")]);
   let count = s
      .zunionstore("dst", &["a", "b"], &[], ZAggregate::Sum)
      .unwrap();
   assert_eq!(count, 3);
   assert_eq!(s.zscore("dst", "x"), Ok(Some(1.0)));
   assert_eq!(s.zscore("dst", "y"), Ok(Some(5.0)));
   assert_eq!(s.zscore("dst", "z"), Ok(Some(4.0)));
}

#[test]
fn zinterstore_intersects() {
   let s = store();
   zadd_simple(&s, "a", &[(1.0, "x"), (2.0, "y"), (3.0, "z")]);
   zadd_simple(&s, "b", &[(10.0, "y"), (20.0, "z")]);
   let count = s
      .zinterstore("dst", &["a", "b"], &[], ZAggregate::Sum)
      .unwrap();
   assert_eq!(count, 2);
   assert_eq!(s.zscore("dst", "y"), Ok(Some(12.0)));
   assert_eq!(s.zscore("dst", "z"), Ok(Some(23.0)));
}

#[test]
fn zinterstore_with_weights() {
   let s = store();
   zadd_simple(&s, "a", &[(1.0, "x")]);
   zadd_simple(&s, "b", &[(2.0, "x")]);
   let count = s
      .zinterstore("dst", &["a", "b"], &[2.0, 3.0], ZAggregate::Sum)
      .unwrap();
   assert_eq!(count, 1);
   assert_eq!(s.zscore("dst", "x"), Ok(Some(8.0)));
}

#[test]
fn zunionstore_aggregate_min() {
   let s = store();
   zadd_simple(&s, "a", &[(5.0, "x")]);
   zadd_simple(&s, "b", &[(3.0, "x")]);
   s.zunionstore("dst", &["a", "b"], &[], ZAggregate::Min)
      .unwrap();
   assert_eq!(s.zscore("dst", "x"), Ok(Some(3.0)));
}

#[test]
fn zunionstore_aggregate_max() {
   let s = store();
   zadd_simple(&s, "a", &[(5.0, "x")]);
   zadd_simple(&s, "b", &[(3.0, "x")]);
   s.zunionstore("dst", &["a", "b"], &[], ZAggregate::Max)
      .unwrap();
   assert_eq!(s.zscore("dst", "x"), Ok(Some(5.0)));
}

#[test]
fn zadd_wrong_type() {
   let s = store();
   set_str(&s, "k", "val");
   let m = vec![(1.0, "a")];
   assert_eq!(s.zadd("k", &m, ZAddOptions::default()), Err("WRONGTYPE"));
}

#[test]
fn zrem_wrong_type() {
   let s = store();
   set_str(&s, "k", "val");
   assert_eq!(s.zrem("k", &["a"]), Err("WRONGTYPE"));
}

#[test]
fn zscore_wrong_type() {
   let s = store();
   set_str(&s, "k", "val");
   assert_eq!(s.zscore("k", "a"), Err("WRONGTYPE"));
}

#[test]
fn zcard_wrong_type() {
   let s = store();
   set_str(&s, "k", "val");
   assert_eq!(s.zcard("k"), Err("WRONGTYPE"));
}

#[test]
fn zdiff_empty_keys_returns_empty() {
   let s = store();
   assert_eq!(s.zdiff(&[]), Ok(vec![]));
}

#[test]
fn zdiff_single_key_returns_first_key() {
   let s = store();
   zadd_simple(&s, "k1", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]);
   let result = s.zdiff(&["k1"]).unwrap();

   assert_eq!(result.len(), 3);
   assert_eq!(result[0].1, 1.0);
   assert_eq!(result[0].0, "a");
   assert_eq!(result[1].1, 2.0);
   assert_eq!(result[1].0, "b");
   assert_eq!(result[2].1, 3.0);
   assert_eq!(result[2].0, "c");
}

#[test]
fn zdiff_basic_difference() {
   let s = store();
   zadd_simple(&s, "k1", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]);
   zadd_simple(&s, "k2", &[(4.0, "d"), (5.0, "e"), (6.0, "f")]);

   let result = s.zdiff(&["k1", "k2"]).unwrap();

   // All items from k1 minus all items in k2 (no overlap), should return k1
   assert_eq!(result.len(), 3);
}

#[test]
fn zdiff_removes_common_members() {
   let s = store();
   zadd_simple(&s, "k1", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]);
   zadd_simple(&s, "k2", &[(4.0, "b"), (5.0, "c"), (6.0, "d")]);

   let result = s.zdiff(&["k1", "k2"]).unwrap();

   // Should return only items in k1 that are NOT in k2: just "a" with score 1.0
   assert_eq!(result.len(), 1);
   assert_eq!(result[0].0, "a");
   assert_eq!(result[0].1, 1.0);
}

#[test]
fn zdiff_multiple_keys_removes_all_common() {
   let s = store();
   zadd_simple(&s, "k1", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]);
   zadd_simple(&s, "k2", &[(4.0, "b"), (5.0, "d")]);
   zadd_simple(&s, "k3", &[(6.0, "c"), (7.0, "e")]);

   let result = s.zdiff(&["k1", "k2", "k3"]).unwrap();

   // Should return only items in k1 that are NOT in ANY of k2 or k3: just "a" with score 1.0
   assert_eq!(result.len(), 1);
   assert_eq!(result[0].0, "a");
   assert_eq!(result[0].1, 1.0);
}

#[test]
fn zdiff_all_common_members_returns_empty() {
   let s = store();
   zadd_simple(&s, "k1", &[(1.0, "a"), (2.0, "b")]);
   zadd_simple(&s, "k2", &[(3.0, "a"), (4.0, "b")]);

   let result = s.zdiff(&["k1", "k2"]).unwrap();

   // All members of k1 are also in k2, so result should be empty
   assert_eq!(result.len(), 0);
}

#[test]
fn zdiff_nonexistent_keys_ignored() {
   let s = store();
   zadd_simple(&s, "k1", &[(1.0, "a"), (2.0, "b")]);

   let result = s.zdiff(&["k1", "nonexistent_key"] as &[&str]).unwrap();

   // Non-existent keys should be ignored, return all members of k1
   assert_eq!(result.len(), 2);
}

#[test]
fn zdiff_nonexistent_all_keys_returns_empty() {
   let s = store();

   let result = s.zdiff(&["nonexistent_key1", "nonexistent_key2"]).unwrap();

   // First key doesn't exist, should return empty vec (not error)
   assert_eq!(result.len(), 0);
}

#[test]
fn zdiff_first_key_missing_returns_empty() {
   let s = store();
   zadd_simple(&s, "k2", &[(1.0, "a")]);

   // First key doesn't exist, should return empty vec
   assert_eq!(s.zdiff(&["missing_first_key"] as &[&str]).unwrap().len(), 0);
}

#[test]
fn zdiff_expired_keys_ignored() {
   let s = store();
   zadd_simple(&s, "k1", &[(1.0, "a"), (2.0, "b")]);
   zadd_simple(&s, "k2", &[(3.0, "a")]);

   // Mark k2 as expired by deleting it
   s.zrem("k2", &["a"]).unwrap();

   let result = s.zdiff(&["k1", "k2"] as &[&str]).unwrap();

   // Expired keys should be ignored, return all members of k1
   assert_eq!(result.len(), 2);
}

#[test]
fn zdiff_wrong_type_key_returns_error() {
   let s = store();
   zadd_simple(&s, "k1", &[(1.0, "a")]);
   set_str(&s, "k2", "not a zset");

   assert_eq!(s.zdiff(&["k1", "k2"] as &[&str]), Err("WRONGTYPE"));
}

#[test]
fn zdiff_wrong_type_second_key_returns_error() {
   let s = store();
   zadd_simple(&s, "k1", &[(1.0, "a")]);
   set_str(&s, "k2", "not a zset");

   assert_eq!(s.zdiff(&["k1", "k2"] as &[&str]), Err("WRONGTYPE"));
}

#[test]
fn zdiff_wrong_type_key_returns_err_for_each_wrong() {
   let s = store();
   zadd_simple(&s, "k1", &[(1.0, "a")]);
   set_str(&s, "k2", "not a zset");
   zadd_simple(&s, "k3", &[(2.0, "b")]);

   // Should fail on the first wrong type encountered (k2)
   assert_eq!(s.zdiff(&["k1", "k2", "k3"] as &[&str]), Err("WRONGTYPE"));
}

#[test]
fn zdiff_mixed_existing_and_nonexistent_keys() {
   let s = store();
   zadd_simple(&s, "k1", &[(1.0, "a"), (2.0, "b")]);
   zadd_simple(&s, "k3", &[(3.0, "a")]);

   // k2 doesn't exist, should be ignored; only remove common members between k1 and k3
   let result = s.zdiff(&["k1", "missing_key", "k3"] as &[&str]).unwrap();

   // Should return only "b" (in k1 but not in k3)
   assert_eq!(result.len(), 1);
   assert_eq!(result[0].0, "b");
}

// #[test]
// fn zdiff_large_sets_with_partial_overlap() {
//     let s = store();
//     let str: &str = "abc";
//     // Create a large set with many members
//     let mut members: Vec<(f64, &str)> = vec![];
//     for i in 0..100 {
//         members.push((i as f64, str));
//     }
//     zadd_simple(&s, "k1", &members);
//
//     // Second set with overlapping members (even numbers)
//     let mut members2: Vec<(f64, &str)> = vec![];
//     for i in (0..100).filter(|&i| i % 2 == 0) {
//         members2.push((i as f64, format!("{}", i).as_str()));
//     }
//     zadd_simple(&s, "k2", &members2);
//
//     let result = s.zdiff(&["k1", "k2"]).unwrap();
//
//     // Should return only odd-numbered members from k1 (50 items)
//     assert_eq!(result.len(), 50);
// }

#[test]
fn zdiff_preserves_scores() {
   let s = store();
   zadd_simple(&s, "k1", &[(1.5, "a"), (2.5, "b"), (3.5, "c")]);
   zadd_simple(&s, "k2", &[(4.0, "b"), (5.0, "d")]);

   let result = s.zdiff(&["k1", "k2"]).unwrap();

   // Should return "a" with score 1.5 and "c" with score 3.5
   assert_eq!(result.len(), 2);
}

#[test]
fn zdiff_single_common_member_removed() {
   let s = store();
   zadd_simple(&s, "k1", &[(1.0, "a"), (2.0, "b")]);
   zadd_simple(&s, "k2", &[(3.0, "b")]);

   let result = s.zdiff(&["k1", "k2"]).unwrap();

   // Should return only "a" with score 1.0
   assert_eq!(result.len(), 1);
   assert_eq!(result[0].0, "a");
   assert_eq!(result[0].1, 1.0);
}

#[test]
fn zdiff_multiple_keys_with_different_common_members() {
   let s = store();
   // k1 has a, b, c
   zadd_simple(&s, "k1", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]);
   // k2 has b, d
   zadd_simple(&s, "k2", &[(4.0, "b"), (5.0, "d")]);
   // k3 has c, e
   zadd_simple(&s, "k3", &[(6.0, "c"), (7.0, "e")]);

   let result = s.zdiff(&["k1", "k2", "k3"]).unwrap();

   // Should return only "a" (not in k2 or k3) with score 1.0
   assert_eq!(result.len(), 1);
   assert_eq!(result[0].0, "a");
   assert_eq!(result[0].1, 1.0);
}

#[test]
fn zdiff_key_with_same_name_in_different_positions() {
   let s = store();
   zadd_simple(&s, "k1", &[(1.0, "a"), (2.0, "b")]);
   zadd_simple(&s, "k2", &[(3.0, "b")]);

   // Same keys in different order - first key is k2, second is k1
   let result = s.zdiff(&["k2", "k1"] as &[&str]).unwrap();

   // Should return only "c" (in k2 but not in k1) with score 3.0... wait, k2 has b and k1 has a,b
   // So result should be empty since b is in both
   assert_eq!(result.len(), 0);
}

#[test]
fn zdiff_empty_result_key() {
   let s = store();
   zadd_simple(&s, "k1", &[(1.0, "a"), (2.0, "b")]);
   zadd_simple(&s, "k2", &[(3.0, "a"), (4.0, "b")]);

   let result = s.zdiff(&["k1", "k2"]).unwrap();

   // All members in common, result should be empty
   assert_eq!(result.len(), 0);
}

#[test]
fn zdiffstore_tracks_partial_and_empty_results() {
   let s = store();
   zadd_simple(&s, "source", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]);
   zadd_simple(&s, "remove_one", &[(4.0, "b")]);
   zadd_simple(&s, "remove_rest", &[(5.0, "a"), (6.0, "c")]);

   assert_eq!(s.zdiffstore("dst", &["source", "remove_one"]), Ok(2));
   assert_eq!(s.zcard("dst"), Ok(2));
   assert_eq!(s.zscore("dst", "a"), Ok(Some(1.0)));
   assert_eq!(s.zscore("dst", "c"), Ok(Some(3.0)));

   assert_eq!(s.zdiffstore("dst", &["dst", "remove_rest"]), Ok(0));
   assert_eq!(s.zcard("dst"), Ok(0));
}
