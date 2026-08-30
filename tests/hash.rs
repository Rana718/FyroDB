mod common;
use common::*;
use fyro_db::storage::value::StoreValue;

fn hset(s: &fyro_db::storage::store::Store, key: &str, pairs: &[(&str, &str)]) {
    s.hset(key, pairs).unwrap();
}

// HSET / HGET

#[test]
fn hset_returns_new_field_count() {
    let s = store();
    assert_eq!(s.hset("k", &[("a", "1"), ("b", "2")]), Ok(2));
    assert_eq!(s.hset("k", &[("a", "updated"), ("c", "3")]), Ok(1));
}

#[test]
fn hget_existing_field() {
    let s = store();
    hset(&s, "k", &[("name", "rana")]);
    assert_eq!(s.hget("k", "name"), Ok(Some("rana".into())));
}

#[test]
fn hget_missing_field_returns_none() {
    let s = store();
    hset(&s, "k", &[("a", "1")]);
    assert_eq!(s.hget("k", "missing"), Ok(None));
}

#[test]
fn hget_missing_key_returns_none() {
    let s = store();
    assert_eq!(s.hget("nope", "f"), Ok(None));
}

// HSETNX

#[test]
fn hsetnx_sets_new_field() {
    let s = store();
    assert_eq!(s.hsetnx("k", "f", "v".into()), Ok(true));
    assert_eq!(s.hget("k", "f"), Ok(Some("v".into())));
}

#[test]
fn hsetnx_does_not_overwrite() {
    let s = store();
    hset(&s, "k", &[("f", "original")]);
    assert_eq!(s.hsetnx("k", "f", "new".into()), Ok(false));
    assert_eq!(s.hget("k", "f"), Ok(Some("original".into())));
}

// HMGET

#[test]
fn hmget_mixed_present_and_missing() {
    let s = store();
    hset(&s, "k", &[("a", "1"), ("b", "2")]);
    let fields = vec!["a", "missing", "b"];
    assert_eq!(
        s.hmget("k", &fields),
        Ok(vec![Some("1".into()), None, Some("2".into())])
    );
}

#[test]
fn hmget_missing_key_all_nil() {
    let s = store();
    let fields = vec!["a", "b"];
    assert_eq!(s.hmget("nope", &fields), Ok(vec![None, None]));
}

// HGETALL

#[test]
fn hgetall_returns_all_pairs() {
    let s = store();
    hset(&s, "k", &[("x", "1"), ("y", "2")]);
    let mut all = s.hgetall("k").unwrap();
    all.sort();
    assert_eq!(
        all,
        vec![("x".into(), "1".into()), ("y".into(), "2".into())]
    );
}

#[test]
fn hgetall_missing_key_empty() {
    let s = store();
    assert_eq!(s.hgetall("nope"), Ok(vec![]));
}

// HDEL

#[test]
fn hdel_removes_and_counts() {
    let s = store();
    hset(&s, "k", &[("a", "1"), ("b", "2"), ("c", "3")]);
    let fields = vec!["a", "c", "nope"];
    assert_eq!(s.hdel("k", &fields), Ok(2));
    assert_eq!(s.hget("k", "a"), Ok(None));
    assert_eq!(s.hget("k", "b"), Ok(Some("2".into())));
}

// HEXISTS

#[test]
fn hexists_true_and_false() {
    let s = store();
    hset(&s, "k", &[("f", "v")]);
    assert_eq!(s.hexists("k", "f"), Ok(true));
    assert_eq!(s.hexists("k", "nope"), Ok(false));
    assert_eq!(s.hexists("nokey", "f"), Ok(false));
}

// HLEN

#[test]
fn hlen_counts_fields() {
    let s = store();
    hset(&s, "k", &[("a", "1"), ("b", "2")]);
    assert_eq!(s.hlen("k"), Ok(2));
    assert_eq!(s.hlen("nope"), Ok(0));
}

// HKEYS / HVALS

#[test]
fn hkeys_and_hvals() {
    let s = store();
    hset(&s, "k", &[("a", "1"), ("b", "2")]);
    let mut keys = s.hkeys("k").unwrap();
    keys.sort();
    assert_eq!(keys, vec!["a", "b"]);
    let mut vals = s.hvals("k").unwrap();
    vals.sort();
    assert_eq!(vals, vec!["1", "2"]);
}

// HINCRBY

#[test]
fn hincrby_creates_and_increments() {
    let s = store();
    assert_eq!(s.hincrby("k", "n", 5), Ok(5));
    assert_eq!(s.hincrby("k", "n", -2), Ok(3));
}

#[test]
fn hincrby_errors_on_non_integer() {
    let s = store();
    hset(&s, "k", &[("f", "abc")]);
    assert!(s.hincrby("k", "f", 1).is_err());
}

#[test]
fn hincrby_overflow_returns_an_error() {
    let s = store();
    hset(&s, "k", &[("f", &i64::MAX.to_string())]);
    assert_eq!(
        s.hincrby("k", "f", 1),
        Err("increment or decrement would overflow")
    );
    assert_eq!(s.hget("k", "f"), Ok(Some(i64::MAX.to_string())));
}

#[test]
fn hincrbyfloat_creates_and_increments() {
    let s = store();
    let r = s.hincrbyfloat("k", "score", 1.5).unwrap();
    assert!((r - 1.5).abs() < 1e-9);
    let r2 = s.hincrbyfloat("k", "score", 0.5).unwrap();
    assert!((r2 - 2.0).abs() < 1e-9);
}

#[test]
fn hincrbyfloat_errors_on_non_float() {
    let s = store();
    hset(&s, "k", &[("f", "notfloat")]);
    assert!(s.hincrbyfloat("k", "f", 1.0).is_err());
}

// WRONGTYPE on all ops

#[test]
fn all_hash_ops_wrongtype_on_string_key() {
    let s = store();
    s.set("k".into(), StoreValue::string("hello".into()));

    assert!(s.hset("k", &[("f", "v")]).is_err());
    assert!(s.hsetnx("k", "f", "v".into()).is_err());
    assert!(s.hget("k", "f").is_err());
    assert!(s.hmget("k", &["f"]).is_err());
    assert!(s.hgetall("k").is_err());
    assert!(s.hdel("k", &["f"]).is_err());
    assert!(s.hexists("k", "f").is_err());
    assert!(s.hlen("k").is_err());
    assert!(s.hkeys("k").is_err());
    assert!(s.hvals("k").is_err());
    assert!(s.hincrby("k", "f", 1).is_err());
    assert!(s.hincrbyfloat("k", "f", 1.0).is_err());
}
