mod common;
use common::*;

fn json_set(s: &fyro_db::storage::store::Store, key: &str, path: &str, val: &str) {
    s.json_set(key, path, val, false, false).unwrap();
}

#[test]
fn json_set_and_get_root() {
    let s = store();
    json_set(&s, "k", "$", r#"{"name":"rana","age":25}"#);
    let result = s.json_get("k", &["$"]).unwrap().unwrap();
    assert!(result.contains("rana"));
    assert!(result.contains("25"));
}

#[test]
fn json_set_nested_path() {
    let s = store();
    json_set(&s, "k", "$", r#"{"a":{"b":1}}"#);
    json_set(&s, "k", "$.a.b", "42");
    let result = s.json_get("k", &["$.a.b"]).unwrap().unwrap();
    assert_eq!(result, "42");
}

#[test]
fn json_get_missing_key() {
    let s = store();
    assert_eq!(s.json_get("k", &["$"]), Ok(None));
}

#[test]
fn json_get_missing_path() {
    let s = store();
    json_set(&s, "k", "$", r#"{"a":1}"#);
    assert_eq!(s.json_get("k", &["$.z"]), Ok(None));
}

#[test]
fn json_del_root_removes_key() {
    let s = store();
    json_set(&s, "k", "$", r#"{"a":1}"#);
    assert_eq!(s.json_del("k", "$"), Ok(1));
    assert_eq!(s.json_get("k", &["$"]), Ok(None));
}

#[test]
fn json_del_nested() {
    let s = store();
    json_set(&s, "k", "$", r#"{"a":1,"b":2}"#);
    assert_eq!(s.json_del("k", "$.a"), Ok(1));
    let result = s.json_get("k", &["$.a"]).unwrap();
    assert_eq!(result, None);
}

#[test]
fn json_del_missing_path() {
    let s = store();
    json_set(&s, "k", "$", r#"{"a":1}"#);
    assert_eq!(s.json_del("k", "$.z"), Ok(0));
}

#[test]
fn json_type_root() {
    let s = store();
    json_set(&s, "k", "$", r#"{"a":1}"#);
    assert_eq!(s.json_type("k", "$"), Ok(Some("object")));
}

#[test]
fn json_type_string() {
    let s = store();
    json_set(&s, "k", "$", r#"{"name":"hello"}"#);
    assert_eq!(s.json_type("k", "$.name"), Ok(Some("string")));
}

#[test]
fn json_type_number() {
    let s = store();
    json_set(&s, "k", "$", r#"{"n":42}"#);
    assert_eq!(s.json_type("k", "$.n"), Ok(Some("number")));
}

#[test]
fn json_type_boolean() {
    let s = store();
    json_set(&s, "k", "$", r#"{"flag":true}"#);
    assert_eq!(s.json_type("k", "$.flag"), Ok(Some("boolean")));
}

#[test]
fn json_type_array() {
    let s = store();
    json_set(&s, "k", "$", r#"{"arr":[1,2,3]}"#);
    assert_eq!(s.json_type("k", "$.arr"), Ok(Some("array")));
}

#[test]
fn json_type_null() {
    let s = store();
    json_set(&s, "k", "$", r#"{"x":null}"#);
    assert_eq!(s.json_type("k", "$.x"), Ok(Some("null")));
}

#[test]
fn json_type_missing_key() {
    let s = store();
    assert_eq!(s.json_type("k", "$"), Ok(None));
}

#[test]
fn json_numincrby_basic() {
    let s = store();
    json_set(&s, "k", "$", r#"{"n":10}"#);
    let result = s.json_numincrby("k", "$.n", 5.0).unwrap().unwrap();
    assert!((result - 15.0).abs() < 0.001);
}

#[test]
fn json_numincrby_negative() {
    let s = store();
    json_set(&s, "k", "$", r#"{"n":10}"#);
    let result = s.json_numincrby("k", "$.n", -3.0).unwrap().unwrap();
    assert!((result - 7.0).abs() < 0.001);
}

#[test]
fn json_numincrby_not_a_number() {
    let s = store();
    json_set(&s, "k", "$", r#"{"s":"hello"}"#);
    assert_eq!(
        s.json_numincrby("k", "$.s", 1.0),
        Err("ERR path value is not a number")
    );
}

#[test]
fn json_nummultby_basic() {
    let s = store();
    json_set(&s, "k", "$", r#"{"n":3}"#);
    let result = s.json_nummultby("k", "$.n", 4.0).unwrap().unwrap();
    assert!((result - 12.0).abs() < 0.001);
}

#[test]
fn json_strappend_basic() {
    let s = store();
    json_set(&s, "k", "$", r#"{"s":"hello"}"#);
    let len = s.json_strappend("k", "$.s", " world").unwrap().unwrap();
    assert_eq!(len, 11);
    let result = s.json_get("k", &["$.s"]).unwrap().unwrap();
    assert!(result.contains("hello world"));
}

#[test]
fn json_strlen_basic() {
    let s = store();
    json_set(&s, "k", "$", r#"{"s":"hello"}"#);
    assert_eq!(s.json_strlen("k", "$.s"), Ok(Some(5)));
}

#[test]
fn json_strlen_missing() {
    let s = store();
    assert_eq!(s.json_strlen("k", "$"), Ok(None));
}

#[test]
fn json_arrappend_basic() {
    let s = store();
    json_set(&s, "k", "$", r#"{"arr":[1,2]}"#);
    let len = s
        .json_arrappend("k", "$.arr", &["3", "4"])
        .unwrap()
        .unwrap();
    assert_eq!(len, 4);
}

#[test]
fn json_arrlen_basic() {
    let s = store();
    json_set(&s, "k", "$", r#"{"arr":[1,2,3]}"#);
    assert_eq!(s.json_arrlen("k", "$.arr"), Ok(Some(3)));
}

#[test]
fn json_arrlen_empty() {
    let s = store();
    json_set(&s, "k", "$", r#"{"arr":[]}"#);
    assert_eq!(s.json_arrlen("k", "$.arr"), Ok(Some(0)));
}

#[test]
fn json_arrpop_last() {
    let s = store();
    json_set(&s, "k", "$", r#"{"arr":[1,2,3]}"#);
    let popped = s.json_arrpop("k", "$.arr", -1).unwrap().unwrap();
    assert_eq!(popped, "3");
    assert_eq!(s.json_arrlen("k", "$.arr"), Ok(Some(2)));
}

#[test]
fn json_arrpop_first() {
    let s = store();
    json_set(&s, "k", "$", r#"{"arr":[1,2,3]}"#);
    let popped = s.json_arrpop("k", "$.arr", 0).unwrap().unwrap();
    assert_eq!(popped, "1");
}

#[test]
fn json_arrindex_found() {
    let s = store();
    json_set(&s, "k", "$", r#"{"arr":[10,20,30,20]}"#);
    assert_eq!(s.json_arrindex("k", "$.arr", "20", 0, 0), Ok(1));
}

#[test]
fn json_arrindex_not_found() {
    let s = store();
    json_set(&s, "k", "$", r#"{"arr":[1,2,3]}"#);
    assert_eq!(s.json_arrindex("k", "$.arr", "99", 0, 0), Ok(-1));
}

#[test]
fn json_arrinsert_basic() {
    let s = store();
    json_set(&s, "k", "$", r#"{"arr":[1,3]}"#);
    let len = s.json_arrinsert("k", "$.arr", 1, &["2"]).unwrap().unwrap();
    assert_eq!(len, 3);
}

#[test]
fn json_arrtrim_basic() {
    let s = store();
    json_set(&s, "k", "$", r#"{"arr":[1,2,3,4,5]}"#);
    let len = s.json_arrtrim("k", "$.arr", 1, 3).unwrap().unwrap();
    assert_eq!(len, 3);
}

#[test]
fn json_objkeys_basic() {
    let s = store();
    json_set(&s, "k", "$", r#"{"a":1,"b":2,"c":3}"#);
    let mut keys = s.json_objkeys("k", "$").unwrap().unwrap();
    keys.sort();
    assert_eq!(keys, vec!["a", "b", "c"]);
}

#[test]
fn json_objlen_basic() {
    let s = store();
    json_set(&s, "k", "$", r#"{"a":1,"b":2}"#);
    assert_eq!(s.json_objlen("k", "$"), Ok(Some(2)));
}

#[test]
fn json_toggle_basic() {
    let s = store();
    json_set(&s, "k", "$", r#"{"flag":true}"#);
    assert_eq!(s.json_toggle("k", "$.flag"), Ok(Some(false)));
    assert_eq!(s.json_toggle("k", "$.flag"), Ok(Some(true)));
}

#[test]
fn json_clear_object() {
    let s = store();
    json_set(&s, "k", "$", r#"{"a":1,"b":2}"#);
    assert_eq!(s.json_clear("k", "$"), Ok(1));
    assert_eq!(s.json_objlen("k", "$"), Ok(Some(0)));
}

#[test]
fn json_clear_array() {
    let s = store();
    json_set(&s, "k", "$", r#"{"arr":[1,2,3]}"#);
    assert_eq!(s.json_clear("k", "$.arr"), Ok(1));
    assert_eq!(s.json_arrlen("k", "$.arr"), Ok(Some(0)));
}

#[test]
fn json_set_nx_does_not_overwrite() {
    let s = store();
    json_set(&s, "k", "$", r#"{"a":1}"#);
    let result = s.json_set("k", "$.a", "99", true, false).unwrap();
    assert!(!result);
    let val = s.json_get("k", &["$.a"]).unwrap().unwrap();
    assert_eq!(val, "1");
}

#[test]
fn json_set_xx_only_if_exists() {
    let s = store();
    let result = s.json_set("k", "$", r#"{"a":1}"#, false, true).unwrap();
    assert!(!result);
    assert_eq!(s.json_get("k", &["$"]), Ok(None));
}

#[test]
fn json_wrong_type() {
    let s = store();
    set_str(&s, "k", "val");
    assert_eq!(s.json_get("k", &["$"]), Err("WRONGTYPE"));
    assert_eq!(s.json_type("k", "$"), Err("WRONGTYPE"));
}

#[test]
fn json_parse_nested_arrays() {
    let s = store();
    json_set(&s, "k", "$", r#"{"matrix":[[1,2],[3,4]]}"#);
    assert_eq!(s.json_type("k", "$.matrix"), Ok(Some("array")));
    assert_eq!(s.json_arrlen("k", "$.matrix"), Ok(Some(2)));
}

#[test]
fn json_get_multiple_paths() {
    let s = store();
    json_set(&s, "k", "$", r#"{"a":1,"b":"hello","c":true}"#);
    let result = s.json_get("k", &["$.a", "$.b", "$.c"]).unwrap().unwrap();
    assert!(result.contains("\"$.a\":1"));
    assert!(result.contains("\"$.b\":\"hello\""));
    assert!(result.contains("\"$.c\":true"));
}
