use fyro_db::commands;
use fyro_db::storage::store::Store;
use fyro_db::storage::value::{SmallStr, StoreValue};
use std::sync::Arc;

fn run(store: &Arc<Store>, parts: &[&str]) -> Vec<u8> {
   let mut out = Vec::new();
   commands::execute(parts, store, &mut out);
   out
}

fn bulk(bytes: &[u8]) -> Vec<u8> {
   let mut out = format!("${}\r\n", bytes.len()).into_bytes();
   out.extend_from_slice(bytes);
   out.extend_from_slice(b"\r\n");
   out
}

#[test]
fn binary_numeric_errors_preserve_values_and_allow_later_writes() {
   let store = Arc::new(Store::with_config(2, usize::MAX));
   for size in [5, 23, 24, 64, 1024, 10240] {
      let mut bytes = vec![b'1'; size];
      bytes[size / 2] = 255;
      store.set("s", StoreValue::string_small(SmallStr::from_slice(&bytes)));
      store.hset_bytes("h", &[("field", &bytes)]).unwrap();
      for cmd in ["INCR", "DECR"] {
         assert!(run(&store, &[cmd, "s"]).starts_with(b"-ERR"));
         assert_eq!(run(&store, &["GET", "s"]), bulk(&bytes));
      }
      assert!(run(&store, &["INCRBYFLOAT", "s", "1"]).starts_with(b"-ERR"));
      assert!(run(&store, &["HINCRBY", "h", "field", "1"]).starts_with(b"-ERR"));
      assert!(run(&store, &["HINCRBYFLOAT", "h", "field", "1"]).starts_with(b"-ERR"));
      assert_eq!(run(&store, &["HGET", "h", "field"]), bulk(&bytes));
      assert_eq!(run(&store, &["HDEL", "h", "field"]), b":1\r\n");
      assert_eq!(run(&store, &["SET", "s", "41"]), b"+OK\r\n");
      assert_eq!(run(&store, &["INCR", "s"]), b":42\r\n");
   }
}

#[test]
fn ranges_and_lcs_use_bytes_even_for_split_unicode_and_invalid_utf8() {
   let store = Arc::new(Store::with_config(2, usize::MAX));
   let bytes = [b'a', 0xff, 0xc3, 0xa9, 0, 13, 10, b'z'];
   for key in ["a", "b"] {
      store.set(key, StoreValue::string_small(SmallStr::from_slice(&bytes)));
   }
   for (start, end) in [(0, 0), (1, 1), (2, 2), (2, 3), (4, 6), (0, 7)] {
      assert_eq!(
         run(
            &store,
            &["GETRANGE", "a", &start.to_string(), &end.to_string()]
         ),
         bulk(&bytes[start..=end])
      );
   }
   assert_eq!(
      run(&store, &["GETRANGE", "a", "-2", "-1"]),
      bulk(&bytes[6..])
   );
   assert_eq!(run(&store, &["LCS", "a", "b"]), bulk(&bytes));
   assert_eq!(run(&store, &["LCS", "a", "b", "LEN"]), b":8\r\n");
   assert_eq!(run(&store, &["GETDEL", "a"]), bulk(&bytes));
}

#[test]
fn binary_small_strings_have_safe_equality_hashing_and_checked_text_views() {
   use std::hash::{DefaultHasher, Hash, Hasher};
   for bytes in [&b"\xff\x00abc"[..], &vec![255; 10240]] {
      let a = SmallStr::from_slice(bytes);
      let b = a.clone();
      assert_eq!(a, b);
      assert!(a.try_as_str().is_err());
      let mut ah = DefaultHasher::new();
      let mut bh = DefaultHasher::new();
      a.hash(&mut ah);
      b.hash(&mut bh);
      assert_eq!(ah.finish(), bh.finish());
      assert!(!format!("{a:?}").is_empty());
      assert_eq!(b.into_bytes(), bytes);
   }
}

#[test]
fn binary_hash_values_survive_full_map_demotion_and_deletion() {
   let store = Arc::new(Store::with_config(2, usize::MAX));
   let fields: Vec<_> = (0..100).map(|i| format!("field:{i}")).collect();
   let value = vec![255; 10240];
   let pairs: Vec<_> = fields
      .iter()
      .map(|f| (f.as_str(), value.as_slice()))
      .collect();
   assert_eq!(store.hset_bytes("full", &pairs), Ok(100));
   for (i, field) in fields.iter().enumerate() {
      assert_eq!(run(&store, &["HGET", "full", field]), bulk(&value));
      assert_eq!(run(&store, &["HDEL", "full", field]), b":1\r\n");
      assert_eq!(
         run(&store, &["HLEN", "full"]),
         format!(":{}\r\n", 99 - i).as_bytes()
      );
   }
}
