mod common;
use common::*;

#[test]
fn integer_set_preserves_canonical_string_semantics() {
    let s = store();
    assert_eq!(s.sadd("ints", &["1", "2", "-3", "01"]), Ok(4));
    assert_eq!(s.sismember("ints", "1"), Ok(true));
    assert_eq!(s.sismember("ints", "01"), Ok(true));
    assert_eq!(s.sismember("ints", "+1"), Ok(false));
    let mut members = s.smembers("ints").unwrap();
    members.sort();
    assert_eq!(members, vec!["-3", "01", "1", "2"]);
}

#[test]
fn sadd_creates_set() {
    let s = store();
    assert_eq!(s.sadd("k", &["a", "b", "c"]), Ok(3));
    assert_eq!(s.scard("k"), Ok(3));
}

#[test]
fn sadd_ignores_duplicates() {
    let s = store();
    assert_eq!(s.sadd("k", &["a", "b"]), Ok(2));
    assert_eq!(s.sadd("k", &["b", "c"]), Ok(1));
    assert_eq!(s.scard("k"), Ok(3));
}

#[test]
fn srem_removes_members() {
    let s = store();
    s.sadd("k", &["a", "b", "c"]).unwrap();
    assert_eq!(s.srem("k", &["a", "c"]), Ok(2));
    assert_eq!(s.scard("k"), Ok(1));
}

#[test]
fn srem_missing_members_returns_zero() {
    let s = store();
    s.sadd("k", &["a"]).unwrap();
    assert_eq!(s.srem("k", &["z"]), Ok(0));
}

#[test]
fn srem_missing_key_returns_zero() {
    let s = store();
    assert_eq!(s.srem("k", &["a"]), Ok(0));
}

#[test]
fn sismember_true() {
    let s = store();
    s.sadd("k", &["a", "b"]).unwrap();
    assert_eq!(s.sismember("k", "a"), Ok(true));
}

#[test]
fn sismember_false() {
    let s = store();
    s.sadd("k", &["a"]).unwrap();
    assert_eq!(s.sismember("k", "z"), Ok(false));
}

#[test]
fn sismember_missing_key() {
    let s = store();
    assert_eq!(s.sismember("k", "a"), Ok(false));
}

#[test]
fn smismember_mixed() {
    let s = store();
    s.sadd("k", &["a", "b"]).unwrap();
    assert_eq!(
        s.smismember("k", &["a", "z", "b"]),
        Ok(vec![true, false, true])
    );
}

#[test]
fn smismember_to_buf_matches_smismember() {
    let s = store();
    s.sadd("k", &["a", "b"]).unwrap();
    let mut out = Vec::new();
    s.smismember_to_buf("k", &["a", "z", "b"], &mut out).unwrap();
    assert_eq!(out, b"*3\r\n:1\r\n:0\r\n:1\r\n");

    let mut out = Vec::new();
    s.smismember_to_buf("nope", &["a", "b"], &mut out).unwrap();
    assert_eq!(out, b"*2\r\n:0\r\n:0\r\n");
}

#[test]
fn smismember_to_buf_wrongtype_errors_without_leaking_bytes() {
    let s = store();
    s.set_string("strk", "x", 0);
    let mut out = Vec::new();
    assert_eq!(
        s.smismember_to_buf("strk", &["a"], &mut out),
        Err("WRONGTYPE")
    );
    assert!(out.is_empty());
}

#[test]
fn smembers_returns_all() {
    let s = store();
    s.sadd("k", &["a", "b", "c"]).unwrap();
    let mut members = s.smembers("k").unwrap();
    members.sort();
    assert_eq!(members, vec!["a", "b", "c"]);
}

#[test]
fn smembers_missing_key_empty() {
    let s = store();
    assert_eq!(s.smembers("k"), Ok(vec![]));
}

#[test]
fn scard_empty() {
    let s = store();
    assert_eq!(s.scard("k"), Ok(0));
}

#[test]
fn spop_removes_member() {
    let s = store();
    s.sadd("k", &["a"]).unwrap();
    let popped = s.spop("k", 1).unwrap();
    assert_eq!(popped, vec!["a"]);
    assert_eq!(s.scard("k"), Ok(0));
}

#[test]
fn spop_empty_returns_empty() {
    let s = store();
    assert_eq!(s.spop("k", 1), Ok(vec![]));
}

#[test]
fn srandmember_returns_member() {
    let s = store();
    s.sadd("k", &["a", "b", "c"]).unwrap();
    let r = s.srandmember("k", 1).unwrap();
    assert_eq!(r.len(), 1);
    assert!(["a", "b", "c"].contains(&r[0].as_str()));
}

#[test]
fn srandmember_negative_allows_duplicates() {
    let s = store();
    s.sadd("k", &["a"]).unwrap();
    let r = s.srandmember("k", -5).unwrap();
    assert_eq!(r.len(), 5);
    assert!(r.iter().all(|m| m == "a"));
}

#[test]
fn smove_moves_member() {
    let s = store();
    s.sadd("src", &["a", "b"]).unwrap();
    assert_eq!(s.smove("src", "dst", "a"), Ok(true));
    assert_eq!(s.sismember("src", "a"), Ok(false));
    assert_eq!(s.sismember("dst", "a"), Ok(true));
}

#[test]
fn smove_nonexistent_member_returns_false() {
    let s = store();
    s.sadd("src", &["a"]).unwrap();
    assert_eq!(s.smove("src", "dst", "z"), Ok(false));
}

#[test]
fn sunion_combines_sets() {
    let s = store();
    s.sadd("a", &["1", "2"]).unwrap();
    s.sadd("b", &["2", "3"]).unwrap();
    let mut result = s.sunion(&["a", "b"]).unwrap();
    result.sort();
    assert_eq!(result, vec!["1", "2", "3"]);
}

#[test]
fn sinter_finds_common() {
    let s = store();
    s.sadd("a", &["1", "2", "3"]).unwrap();
    s.sadd("b", &["2", "3", "4"]).unwrap();
    let mut result = s.sinter(&["a", "b"]).unwrap();
    result.sort();
    assert_eq!(result, vec!["2", "3"]);
}

#[test]
fn sinter_empty_on_disjoint() {
    let s = store();
    s.sadd("a", &["1", "2"]).unwrap();
    s.sadd("b", &["3", "4"]).unwrap();
    assert_eq!(s.sinter(&["a", "b"]), Ok(vec![]));
}

#[test]
fn sdiff_subtracts() {
    let s = store();
    s.sadd("a", &["1", "2", "3"]).unwrap();
    s.sadd("b", &["2", "4"]).unwrap();
    let mut result = s.sdiff(&["a", "b"]).unwrap();
    result.sort();
    assert_eq!(result, vec!["1", "3"]);
}

#[test]
fn sunionstore_stores_result() {
    let s = store();
    s.sadd("a", &["1", "2"]).unwrap();
    s.sadd("b", &["3"]).unwrap();
    assert_eq!(s.sunionstore("dst", &["a", "b"]), Ok(3));
    assert_eq!(s.scard("dst"), Ok(3));
}

#[test]
fn sinterstore_stores_result() {
    let s = store();
    s.sadd("a", &["1", "2"]).unwrap();
    s.sadd("b", &["2", "3"]).unwrap();
    assert_eq!(s.sinterstore("dst", &["a", "b"]), Ok(1));
}

#[test]
fn sdiffstore_stores_result() {
    let s = store();
    s.sadd("a", &["1", "2", "3"]).unwrap();
    s.sadd("b", &["2"]).unwrap();
    assert_eq!(s.sdiffstore("dst", &["a", "b"]), Ok(2));
}

#[test]
fn sintercard_with_limit() {
    let s = store();
    s.sadd("a", &["1", "2", "3"]).unwrap();
    s.sadd("b", &["1", "2", "3"]).unwrap();
    assert_eq!(s.sintercard(&["a", "b"], 2), Ok(2));
}

#[test]
fn sintercard_without_limit() {
    let s = store();
    s.sadd("a", &["1", "2", "3"]).unwrap();
    s.sadd("b", &["1", "2", "3"]).unwrap();
    assert_eq!(s.sintercard(&["a", "b"], 0), Ok(3));
}

#[test]
fn sadd_wrong_type() {
    let s = store();
    set_str(&s, "k", "val");
    assert_eq!(s.sadd("k", &["a"]), Err("WRONGTYPE"));
}

#[test]
fn srem_wrong_type() {
    let s = store();
    set_str(&s, "k", "val");
    assert_eq!(s.srem("k", &["a"]), Err("WRONGTYPE"));
}

#[test]
fn sismember_wrong_type() {
    let s = store();
    set_str(&s, "k", "val");
    assert_eq!(s.sismember("k", "a"), Err("WRONGTYPE"));
}
