mod common;
use common::*;
use fyro_db::storage::rdb;

fn tmp_path(name: &str) -> String {
    format!("/tmp/fyrodb_test_{}.rdb", name)
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.tmp"));
}

#[test]
fn rdb_string_roundtrip() {
    let path = tmp_path("string_roundtrip");
    cleanup(&path);

    let s = store();
    set_str(&s, "name", "rana");
    set_str(&s, "city", "delhi");

    rdb::save(&s, &path).unwrap();

    let s2 = store();
    let count = rdb::load(&s2, &path).unwrap();
    assert_eq!(count, 2);
    assert_eq!(s2.get("name"), Some("rana".into()));
    assert_eq!(s2.get("city"), Some("delhi".into()));

    cleanup(&path);
}

#[test]
fn rdb_hash_roundtrip() {
    let path = tmp_path("hash_roundtrip");
    cleanup(&path);

    let s = store();
    s.hset(
        "user:1",
        vec![("name".into(), "rana".into()), ("age".into(), "25".into())],
    )
    .unwrap();

    rdb::save(&s, &path).unwrap();

    let s2 = store();
    let count = rdb::load(&s2, &path).unwrap();
    assert_eq!(count, 1);
    assert_eq!(s2.hget("user:1", "name"), Ok(Some("rana".into())));
    assert_eq!(s2.hget("user:1", "age"), Ok(Some("25".into())));

    cleanup(&path);
}

#[test]
fn rdb_mixed_types_roundtrip() {
    let path = tmp_path("mixed_roundtrip");
    cleanup(&path);

    let s = store();
    set_str(&s, "str_key", "hello");
    s.hset("hash_key", vec![("f".into(), "v".into())]).unwrap();

    rdb::save(&s, &path).unwrap();

    let s2 = store();
    let count = rdb::load(&s2, &path).unwrap();
    assert_eq!(count, 2);
    assert_eq!(s2.get("str_key"), Some("hello".into()));
    assert_eq!(s2.hget("hash_key", "f"), Ok(Some("v".into())));

    cleanup(&path);
}

#[test]
fn rdb_ttl_preserved() {
    let path = tmp_path("ttl_preserved");
    cleanup(&path);

    let s = store();
    set_expiring(&s, "k", "v", 100);

    rdb::save(&s, &path).unwrap();

    let s2 = store();
    rdb::load(&s2, &path).unwrap();
    assert_eq!(s2.get("k"), Some("v".into()));
    let ttl = s2.ttl("k").unwrap();
    assert!(ttl.as_secs() > 90 && ttl.as_secs() <= 100);
    assert!(s2.has_ttl_keys());

    cleanup(&path);
}

#[test]
fn strict_rdb_load_rejects_truncated_snapshot() {
    let path = tmp_path("strict_truncated");
    cleanup(&path);

    let s = store();
    set_str(&s, "key", "value");
    rdb::save(&s, &path).unwrap();
    let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    let length = file.metadata().unwrap().len();
    file.set_len(length.saturating_sub(1)).unwrap();

    let target = store();
    assert!(rdb::load_strict(&target, &path).is_err());

    cleanup(&path);
}

#[test]
fn rdb_expired_keys_not_loaded() {
    let path = tmp_path("expired_not_loaded");
    cleanup(&path);

    let s = store();
    set_str(&s, "live", "yes");
    set_expired(&s, "dead", "no");
    std::thread::sleep(std::time::Duration::from_millis(5));

    rdb::save(&s, &path).unwrap();

    let s2 = store();
    let count = rdb::load(&s2, &path).unwrap();
    assert_eq!(count, 1);
    assert_eq!(s2.get("live"), Some("yes".into()));
    assert_eq!(s2.get("dead"), None);

    cleanup(&path);
}

// Edge cases

#[test]
fn rdb_empty_store_roundtrip() {
    let path = tmp_path("empty");
    cleanup(&path);

    let s = store();
    rdb::save(&s, &path).unwrap();

    let s2 = store();
    let count = rdb::load(&s2, &path).unwrap();
    assert_eq!(count, 0);
    assert_eq!(s2.dbsize(), 0);

    cleanup(&path);
}

#[test]
fn rdb_load_missing_file_returns_zero() {
    let s = store();
    let count = rdb::load(&s, "/tmp/fyrodb_nonexistent_xyz.rdb").unwrap();
    assert_eq!(count, 0);
}

#[test]
fn rdb_save_is_atomic_tmp_file_cleaned_up() {
    let path = tmp_path("atomic");
    cleanup(&path);

    let s = store();
    set_str(&s, "k", "v");
    rdb::save(&s, &path).unwrap();

    assert!(!std::path::Path::new(&format!("{path}.tmp")).exists());
    assert!(std::path::Path::new(&path).exists());

    cleanup(&path);
}

#[test]
fn rdb_overwrites_existing_file() {
    let path = tmp_path("overwrite");
    cleanup(&path);

    let s = store();
    set_str(&s, "a", "1");
    rdb::save(&s, &path).unwrap();

    let s2 = store();
    set_str(&s2, "b", "2");
    rdb::save(&s2, &path).unwrap();

    let s3 = store();
    let count = rdb::load(&s3, &path).unwrap();
    assert_eq!(count, 1);
    assert_eq!(s3.get("b"), Some("2".into()));
    assert_eq!(s3.get("a"), None);

    cleanup(&path);
}

#[test]
fn rdb_unicode_keys_and_values() {
    let path = tmp_path("unicode");
    cleanup(&path);

    let s = store();
    set_str(&s, "キー", "値");
    set_str(&s, "🔥", "🚀");

    rdb::save(&s, &path).unwrap();

    let s2 = store();
    rdb::load(&s2, &path).unwrap();
    assert_eq!(s2.get("キー"), Some("値".into()));
    assert_eq!(s2.get("🔥"), Some("🚀".into()));

    cleanup(&path);
}
