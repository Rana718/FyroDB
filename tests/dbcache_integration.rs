use fyro_db::storage::store::Store;
use fyro_dbcache::DbCacheManager;
use fyro_dbcache::query::{QueryKind, make_cache_key, parse_sql};
use fyro_dbcache::row::{ColumnInfo, DbResultSet, DbRow, DbValue};
use std::sync::Arc;

#[test]
fn test_cross_protocol_dbcache_and_store_sharing() {
   let store = Arc::new(Store::new());
   let manager = DbCacheManager::new(store.clone());

   let sql = "SELECT id, username, email FROM users WHERE id = 42;";
   let parsed = parse_sql(sql);
   assert_eq!(parsed.kind, QueryKind::Select);
   assert_eq!(parsed.tables, vec!["users".to_string()]);

   let result = DbResultSet {
      columns: vec![
         ColumnInfo {
            name: "id".to_string(),
            type_oid: 23,
         },
         ColumnInfo {
            name: "username".to_string(),
            type_oid: 25,
         },
         ColumnInfo {
            name: "email".to_string(),
            type_oid: 25,
         },
      ],
      rows: vec![DbRow {
         values: vec![
            DbValue::Int(42),
            DbValue::Text("rana".to_string()),
            DbValue::Text("rana@example.com".to_string()),
         ],
      }],
      rows_affected: 1,
   };

   manager.store_cached_query("db:", &parsed.tables, parsed.fingerprint, &result, 60_000);

   // Redis port 8000 access: read raw key directly from Store
   let cache_key = make_cache_key("db:", "users", parsed.fingerprint);
   let redis_val = store.get(&cache_key).expect("key must exist in Store");

   assert_eq!(
      redis_val,
      r#"[{"id":42,"username":"rana","email":"rana@example.com"}]"#
   );

   // Write on users table triggers invalidation in Store
   let write_sql = "UPDATE users SET email = 'new@example.com' WHERE id = 42;";
   let write_parsed = parse_sql(write_sql);
   let evicted = manager.invalidate_tables(&write_parsed.tables);
   assert_eq!(evicted, 1);

   // Key must now be deleted from Store
   assert!(store.get(&cache_key).is_none());
}

#[test]
fn test_dbcache_stats_over_resp_port() {
   let store = Arc::new(Store::new());
   let manager = Arc::new(DbCacheManager::new(store.clone()));
   store.install_dbcache_manager(Arc::clone(&manager));

   let parsed = parse_sql("SELECT id, username FROM users");
   let result = DbResultSet {
      columns: vec![ColumnInfo {
         name: "id".to_string(),
         type_oid: 23,
      }],
      rows: vec![DbRow {
         values: vec![DbValue::Int(42)],
      }],
      rows_affected: 1,
   };
   manager.store_cached_query("db:", &parsed.tables, parsed.fingerprint, &result, 60_000);

   let mut out = Vec::new();
   fyro_db::commands::execute(&["DBCACHE", "STATS"], &store, &mut out);
   let text = String::from_utf8(out).expect("RESP is ascii");
   assert!(text.starts_with("*34\r\n"), "unexpected header: {text}");
   assert!(text.contains("$5\r\nfills\r\n:1\r\n"));
   assert!(text.contains("$12\r\ntracked_keys\r\n:1\r\n"));
}

#[test]
fn test_dbcache_stats_requires_enabled_manager() {
   let store = Arc::new(Store::new());
   let mut out = Vec::new();
   fyro_db::commands::execute(&["DBCACHE", "STATS"], &store, &mut out);
   assert_eq!(
      String::from_utf8(out).unwrap(),
      "-ERR dbcache is not enabled\r\n"
   );
}
