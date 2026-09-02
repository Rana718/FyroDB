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
    assert_eq!(
        s.zadd("k", &members, ZAddOptions::default()),
        Ok(3)
    );
    assert_eq!(s.zcard("k"), Ok(3));
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
    s.zadd("k", &m, ZAddOptions { nx: true, ..Default::default() }).unwrap();
    assert_eq!(s.zscore("k", "a"), Ok(Some(1.0)));
}

#[test]
fn zadd_xx_only_updates_existing() {
    let s = store();
    zadd_simple(&s, "k", &[(1.0, "a")]);
    let m = vec![(5.0, "a"), (3.0, "b")];
    let added = s.zadd("k", &m, ZAddOptions { xx: true, ..Default::default() }).unwrap();
    assert_eq!(added, 0);
    assert_eq!(s.zscore("k", "a"), Ok(Some(5.0)));
    assert_eq!(s.zscore("k", "b"), Ok(None));
}

#[test]
fn zadd_gt_only_increases() {
    let s = store();
    zadd_simple(&s, "k", &[(5.0, "a")]);
    let m = vec![(3.0, "a")];
    s.zadd("k", &m, ZAddOptions { gt: true, ..Default::default() }).unwrap();
    assert_eq!(s.zscore("k", "a"), Ok(Some(5.0)));
    let m2 = vec![(10.0, "a")];
    s.zadd("k", &m2, ZAddOptions { gt: true, ..Default::default() }).unwrap();
    assert_eq!(s.zscore("k", "a"), Ok(Some(10.0)));
}

#[test]
fn zadd_lt_only_decreases() {
    let s = store();
    zadd_simple(&s, "k", &[(5.0, "a")]);
    let m = vec![(10.0, "a")];
    s.zadd("k", &m, ZAddOptions { lt: true, ..Default::default() }).unwrap();
    assert_eq!(s.zscore("k", "a"), Ok(Some(5.0)));
    let m2 = vec![(2.0, "a")];
    s.zadd("k", &m2, ZAddOptions { lt: true, ..Default::default() }).unwrap();
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
    assert_eq!(
        s.zmscore_to_buf("strk", &["a"], &mut out),
        Err("WRONGTYPE")
    );
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
    assert_eq!(
        s.zadd("k", &m, ZAddOptions::default()),
        Err("WRONGTYPE")
    );
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
