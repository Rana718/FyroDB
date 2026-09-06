mod common;
use common::*;

// DBSIZE

#[test]
fn dbsize_empty() {
    let s = store();
    assert_eq!(s.dbsize(), 0);
}

#[test]
fn dbsize_counts_keys() {
    let s = store();
    set_str(&s, "a", "1");
    set_str(&s, "b", "2");
    assert_eq!(s.dbsize(), 2);
}

// FLUSH

#[test]
fn flush_clears_all_keys() {
    let s = store();
    set_str(&s, "a", "1");
    set_str(&s, "b", "2");
    s.flush();
    assert_eq!(s.dbsize(), 0);
}

// TYPE

#[test]
fn type_of_string_key() {
    let s = store();
    set_str(&s, "k", "v");
    assert_eq!(s.type_of("k"), "string");
}

#[test]
fn type_of_hash_key() {
    let s = store();
    s.hset("k", &[("f", "v")]).unwrap();
    assert_eq!(s.type_of("k"), "hash");
}

#[test]
fn type_of_missing_key() {
    let s = store();
    assert_eq!(s.type_of("nope"), "none");
}

#[test]
fn type_of_expired_key_returns_none() {
    let s = store();
    set_expired(&s, "k", "v");
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert_eq!(s.type_of("k"), "none");
}

// CONNECTED CLIENTS

#[test]
fn client_counter_increments_and_decrements() {
    let s = store();
    assert_eq!(s.connected_clients(), 0);
    s.client_connected();
    s.client_connected();
    assert_eq!(s.connected_clients(), 2);
    s.client_disconnected();
    assert_eq!(s.connected_clients(), 1);
}

// CLEANUP EXPIRED

#[test]
fn cleanup_expired_removes_stale_keys() {
    let s = store();
    set_str(&s, "live", "v");
    set_expired(&s, "dead", "v");
    std::thread::sleep(std::time::Duration::from_millis(5));
    s.cleanup_expired();
    assert_eq!(s.dbsize(), 1);
    assert!(s.exists("live"));
}
