mod common;
use common::*;

#[test]
fn pfadd_creates_key() {
    let s = store();
    assert_eq!(s.pfadd("k", &["a", "b", "c"]), Ok(true));
}

#[test]
fn pfadd_duplicate_returns_false() {
    let s = store();
    s.pfadd("k", &["a", "b"]).unwrap();
    assert_eq!(s.pfadd("k", &["a", "b"]), Ok(false));
}

#[test]
fn pfadd_new_element_returns_true() {
    let s = store();
    s.pfadd("k", &["a"]).unwrap();
    assert_eq!(s.pfadd("k", &["b"]), Ok(true));
}

#[test]
fn pfcount_empty() {
    let s = store();
    assert_eq!(s.pfcount(&["k"]), Ok(0));
}

#[test]
fn pfcount_basic() {
    let s = store();
    s.pfadd("k", &["a", "b", "c", "d", "e"]).unwrap();
    let count = s.pfcount(&["k"]).unwrap();
    assert!((4..=6).contains(&count));
}

#[test]
fn pfcount_large_cardinality() {
    let s = store();
    let elements: Vec<String> = (0..1000).map(|i| format!("elem{}", i)).collect();
    let refs: Vec<&str> = elements.iter().map(|s| s.as_str()).collect();
    s.pfadd("k", &refs).unwrap();
    let count = s.pfcount(&["k"]).unwrap();
    assert!((900..=1100).contains(&count));
}

#[test]
fn pfcount_multiple_keys() {
    let s = store();
    s.pfadd("a", &["1", "2", "3"]).unwrap();
    s.pfadd("b", &["3", "4", "5"]).unwrap();
    let count = s.pfcount(&["a", "b"]).unwrap();
    assert!((4..=6).contains(&count));
}

#[test]
fn pfmerge_combines() {
    let s = store();
    s.pfadd("a", &["1", "2", "3"]).unwrap();
    s.pfadd("b", &["4", "5", "6"]).unwrap();
    s.pfmerge("dst", &["a", "b"]).unwrap();
    let count = s.pfcount(&["dst"]).unwrap();
    assert!((5..=7).contains(&count));
}

#[test]
fn pfmerge_with_overlap() {
    let s = store();
    s.pfadd("a", &["1", "2", "3"]).unwrap();
    s.pfadd("b", &["2", "3", "4"]).unwrap();
    s.pfmerge("dst", &["a", "b"]).unwrap();
    let count = s.pfcount(&["dst"]).unwrap();
    assert!((3..=5).contains(&count));
}

#[test]
fn pfadd_wrong_type() {
    let s = store();
    s.sadd("k", &["member"]).unwrap();
    assert_eq!(s.pfadd("k", &["a"]), Err("WRONGTYPE"));
}

#[test]
fn pfcount_wrong_type() {
    let s = store();
    s.sadd("k", &["member"]).unwrap();
    assert_eq!(s.pfcount(&["k"]), Err("WRONGTYPE"));
}
