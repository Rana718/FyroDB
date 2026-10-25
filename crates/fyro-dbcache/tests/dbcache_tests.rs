use fyro_dbcache::protocol::{
   FrameReader, FrontendMessage, encode_bind, put_message, read_frontend_message, write_result_set,
};
use fyro_dbcache::query::{QueryKind, compute_fingerprint_with_params, make_cache_key, parse_sql};
use fyro_dbcache::row::{ColumnInfo, DbResultSet, DbRow, DbValue};
use fyro_dbcache::{
   CacheLimits, CatalogSnapshot, DbCacheManager, DbCacheStorage, MemoryStorage, ParsedQueryCache,
   SessionChange, TableCounters,
};
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::Ordering;

#[test]
fn test_sql_parsing_and_tables() {
   let select = parse_sql("SELECT id, name FROM users WHERE id = 42;");
   assert_eq!(select.kind, QueryKind::Select);
   assert_eq!(select.tables, vec!["users".to_string()]);
   assert!(select.is_cacheable);

   let insert = parse_sql("INSERT INTO orders (id, total) VALUES (1, 99.5);");
   assert_eq!(insert.kind, QueryKind::Insert);
   assert_eq!(insert.tables, vec!["orders".to_string()]);
   assert!(!insert.is_cacheable);

   let update = parse_sql("UPDATE users SET name = 'bob' WHERE id = 1;");
   assert_eq!(update.kind, QueryKind::Update);
   assert_eq!(update.tables, vec!["users".to_string()]);

   let delete = parse_sql("DELETE FROM sessions WHERE expired = true;");
   assert_eq!(delete.kind, QueryKind::Delete);
   assert_eq!(delete.tables, vec!["sessions".to_string()]);
}

#[test]
fn test_complex_joins_and_multi_table_invalidation() {
   let storage = Arc::new(MemoryStorage::default());
   let manager = DbCacheManager::new(storage);

   let join_sql = "SELECT u.name, o.total, p.title \
                   FROM users u \
                   JOIN orders o ON u.id = o.user_id \
                   JOIN products p ON o.product_id = p.id \
                   WHERE u.id = 10;";
   let parsed = parse_sql(join_sql);

   assert_eq!(parsed.kind, QueryKind::Select);
   assert_eq!(parsed.tables.len(), 3);
   assert!(parsed.tables.contains(&"users".to_string()));
   assert!(parsed.tables.contains(&"orders".to_string()));
   assert!(parsed.tables.contains(&"products".to_string()));
   assert!(parsed.is_cacheable);

   let rs = DbResultSet {
      columns: vec![ColumnInfo {
         name: "title".to_string(),
         type_oid: 25,
      }],
      rows: vec![DbRow {
         values: vec![DbValue::Text("Book".to_string())],
      }],
      rows_affected: 1,
   };

   manager.store_cached_query("db:", &parsed.tables, parsed.fingerprint, &rs, 60_000);
   assert!(
      manager
         .get_cached_query("db:", "users", parsed.fingerprint)
         .is_some()
   );

   // Updating any joined table (e.g. products) must invalidate the cached JOIN query
   let write_sql = "UPDATE products SET price = 20 WHERE id = 1;";
   let write_parsed = parse_sql(write_sql);
   let evicted = manager.invalidate_tables(&write_parsed.tables);
   assert_eq!(evicted, 1);

   assert!(
      manager
         .get_cached_query("db:", "users", parsed.fingerprint)
         .is_none()
   );
}

#[test]
fn test_ctes_and_nested_subqueries() {
   let cte_sql = "WITH active_users AS ( \
                     SELECT id, name FROM users WHERE active = true \
                  ) \
                  SELECT a.name, o.total \
                  FROM active_users a \
                  JOIN orders o ON a.id = o.user_id \
                  WHERE o.amount > (SELECT AVG(amount) FROM benchmarks);";
   let parsed = parse_sql(cte_sql);

   assert_eq!(parsed.kind, QueryKind::Select);
   // CTE name 'active_users' must be excluded; real underlying tables extracted
   assert!(!parsed.tables.contains(&"active_users".to_string()));
   assert!(parsed.tables.contains(&"users".to_string()));
   assert!(parsed.tables.contains(&"orders".to_string()));
   assert!(parsed.tables.contains(&"benchmarks".to_string()));
   assert!(parsed.is_cacheable);
}

#[test]
fn test_volatile_functions_bypass_cache() {
   let sql_now = "SELECT * FROM events WHERE created_at < NOW();";
   let parsed_now = parse_sql(sql_now);
   assert!(!parsed_now.is_cacheable);

   let sql_rand = "SELECT * FROM ads ORDER BY RANDOM() LIMIT 1;";
   let parsed_rand = parse_sql(sql_rand);
   assert!(!parsed_rand.is_cacheable);

   let sql_uuid = "SELECT gen_random_uuid(), name FROM users;";
   let parsed_uuid = parse_sql(sql_uuid);
   assert!(!parsed_uuid.is_cacheable);
}

#[test]
fn test_set_operations_union() {
   let union_sql = "SELECT id FROM orders UNION ALL SELECT id FROM archived_orders;";
   let parsed = parse_sql(union_sql);

   assert_eq!(parsed.kind, QueryKind::Select);
   assert!(parsed.tables.contains(&"orders".to_string()));
   assert!(parsed.tables.contains(&"archived_orders".to_string()));
}

#[test]
fn test_complex_multi_table_updates() {
   let update_from_sql = "UPDATE users SET active = false \
                          FROM accounts \
                          WHERE users.account_id = accounts.id AND accounts.suspended = true;";
   let parsed = parse_sql(update_from_sql);

   assert_eq!(parsed.kind, QueryKind::Update);
   assert!(parsed.tables.contains(&"users".to_string()));
   assert!(parsed.tables.contains(&"accounts".to_string()));
}

#[test]
fn test_sql_fingerprint_normalization() {
   let q1 = parse_sql("SELECT * FROM users WHERE id = 1;");
   let q2 = parse_sql("select * from users where id = 999;");
   assert_eq!(q1.normalized, "select * from users where id = 1;");
   assert_eq!(q2.normalized, "select * from users where id = 999;");
   assert_ne!(q1.fingerprint, q2.fingerprint);

   let q_alice = parse_sql("SELECT * FROM users WHERE name = 'alice';");
   let q_bob = parse_sql("SELECT * FROM users WHERE name = 'bob';");
   let q_alice_spaces = parse_sql("select   *   from   users   where   name = 'alice';");
   assert_ne!(q_alice.fingerprint, q_bob.fingerprint);
   assert_eq!(q_alice.fingerprint, q_alice_spaces.fingerprint);
}

#[test]
fn test_rowset_json_serialization_for_redis() {
   let rs = DbResultSet {
      columns: vec![
         ColumnInfo {
            name: "id".to_string(),
            type_oid: 23,
         },
         ColumnInfo {
            name: "name".to_string(),
            type_oid: 25,
         },
      ],
      rows: vec![
         DbRow {
            values: vec![DbValue::Int(1), DbValue::Text("alice".to_string())],
         },
         DbRow {
            values: vec![DbValue::Int(2), DbValue::Text("bob".to_string())],
         },
      ],
      rows_affected: 2,
   };

   let json = rs.to_json();
   assert_eq!(json, r#"[{"id":1,"name":"alice"},{"id":2,"name":"bob"}]"#);

   let decoded = DbResultSet::from_json(&json).expect("must parse json");
   assert_eq!(decoded.rows.len(), 2);
   assert_eq!(decoded.columns[0].name, "id");
   assert_eq!(decoded.columns[1].name, "name");
}

#[test]
fn test_cache_storage_and_table_invalidation() {
   let storage = Arc::new(MemoryStorage::default());
   let manager = DbCacheManager::new(storage);

   let parsed = parse_sql("SELECT * FROM accounts WHERE id = 10;");
   let rs = DbResultSet {
      columns: vec![ColumnInfo {
         name: "id".to_string(),
         type_oid: 23,
      }],
      rows: vec![DbRow {
         values: vec![DbValue::Int(10)],
      }],
      rows_affected: 1,
   };

   manager.store_cached_query("db:", &parsed.tables, parsed.fingerprint, &rs, 10_000);

   let cached = manager
      .get_cached_query("db:", "accounts", parsed.fingerprint)
      .expect("must be cached");
   assert_eq!(cached.rows.len(), 1);

   // Invalidation on write
   let write = parse_sql("UPDATE accounts SET balance = 0 WHERE id = 10;");
   let evicted = manager.invalidate_tables(&write.tables);
   assert_eq!(evicted, 1);

   assert!(
      manager
         .get_cached_query("db:", "accounts", parsed.fingerprint)
         .is_none()
   );
}

#[test]
fn test_protocol_wire_encoding() {
   let mut buf = Vec::new();
   let rs = DbResultSet {
      columns: vec![ColumnInfo {
         name: "val".to_string(),
         type_oid: 25,
      }],
      rows: vec![DbRow {
         values: vec![DbValue::Text("hello".to_string())],
      }],
      rows_affected: 1,
   };

   write_result_set(&mut buf, &rs, "SELECT 1").expect("must write wire");
   assert_eq!(buf[0], b'T');
}

#[test]
fn test_protocol_startup_reading() {
   let mut packet = Vec::new();
   let payload = b"\x00\x03\x00\x00user\0postgres\0database\0testdb\0\0";
   let len = (payload.len() + 4) as i32;
   packet.extend_from_slice(&len.to_be_bytes());
   packet.extend_from_slice(payload);

   let mut cursor = Cursor::new(packet);
   let msg = read_frontend_message(&mut cursor, true).expect("must parse startup");
   assert_eq!(
      msg,
      FrontendMessage::Startup {
         user: "postgres".to_string(),
         database: "testdb".to_string(),
         params: vec![
            ("user".to_string(), "postgres".to_string()),
            ("database".to_string(), "testdb".to_string()),
         ],
      }
   );
}

#[test]
fn test_encoded_cache_hit_and_invalidation() {
   let storage = Arc::new(MemoryStorage::default());
   let manager = DbCacheManager::new(storage);
   let parsed = parse_sql("SELECT id, name FROM users WHERE id = 3;");
   let rs = DbResultSet {
      columns: vec![
         ColumnInfo {
            name: "id".to_string(),
            type_oid: 23,
         },
         ColumnInfo {
            name: "name".to_string(),
            type_oid: 25,
         },
      ],
      rows: vec![DbRow {
         values: vec![DbValue::Int(3), DbValue::Text("carol".to_string())],
      }],
      rows_affected: 1,
   };

   manager.store_cached_query("db:", &parsed.tables, parsed.fingerprint, &rs, 60_000);
   let encoded = manager
      .get_encoded("db:", "users", parsed.fingerprint)
      .expect("encoded hit");
   assert_eq!(encoded.row_count, 1);
   assert_eq!(encoded.columns.len(), 2);

   // Invalidation must drop the wire cache as well as the JSON entry.
   manager.invalidate_tables(&parsed.tables);
   assert!(
      manager
         .get_encoded("db:", "users", parsed.fingerprint)
         .is_none()
   );
   assert!(
      manager
         .get_cached_query("db:", "users", parsed.fingerprint)
         .is_none()
   );
}

#[test]
fn test_analyze_is_parsed_once() {
   let storage = Arc::new(MemoryStorage::default());
   let manager = DbCacheManager::new(storage);
   let sql = "SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id;";
   let first = manager.analyze(sql);
   let second = manager.analyze(sql);
   assert!(Arc::ptr_eq(&first, &second));
   assert_eq!(manager.parsed_cache_len(), 1);
}

#[test]
fn test_non_cacheable_statement_shapes() {
   let multi = parse_sql("SELECT * FROM users; SELECT * FROM orders;");
   assert!(!multi.is_cacheable);
   assert!(multi.tables.contains(&"users".to_string()));
   assert!(multi.tables.contains(&"orders".to_string()));

   assert!(!parse_sql("SELECT * FROM users FOR UPDATE;").is_cacheable);
   assert!(!parse_sql("SELECT * FROM users FOR SHARE;").is_cacheable);
   assert!(!parse_sql("UPDATE users SET active = true;").is_cacheable);
}

#[test]
fn test_multi_statement_writes_invalidate() {
   let mixed = parse_sql("SELECT * FROM users; UPDATE users SET active = false;");
   assert!(!mixed.is_cacheable);
   assert!(mixed.writes);
   assert!(mixed.tables.contains(&"users".to_string()));

   let read_only = parse_sql("SELECT * FROM users WHERE name = 'update';");
   assert!(read_only.is_cacheable);
   assert!(!read_only.writes);

   let cte = parse_sql("WITH gone AS (DELETE FROM users RETURNING id) SELECT * FROM gone;");
   assert!(!cte.is_cacheable);
   assert!(cte.writes);
   assert!(cte.tables.contains(&"users".to_string()));
}

#[test]
fn test_parameterized_fingerprints() {
   let parsed = parse_sql("SELECT * FROM users WHERE id = $1;");
   assert!(parsed.is_cacheable);

   let first = compute_fingerprint_with_params(&parsed.normalized, &[Some(b"1".to_vec())], &[0]);
   let second = compute_fingerprint_with_params(&parsed.normalized, &[Some(b"2".to_vec())], &[0]);
   assert_ne!(first, second);

   let binary = compute_fingerprint_with_params(&parsed.normalized, &[Some(b"1".to_vec())], &[1]);
   assert_ne!(first, binary);
}

#[test]
fn test_stale_fill_is_dropped_after_invalidation() {
   let storage = Arc::new(MemoryStorage::default());
   let manager = DbCacheManager::new(storage);
   let parsed = parse_sql("SELECT id FROM users WHERE id = 11;");
   let rs = DbResultSet {
      columns: vec![ColumnInfo {
         name: "id".to_string(),
         type_oid: 23,
      }],
      rows: vec![DbRow {
         values: vec![DbValue::Int(11)],
      }],
      rows_affected: 1,
   };

   // Snapshot before the write lands, then invalidate while the SELECT is in
   // flight: the fill must be discarded instead of resurrecting stale rows.
   let versions = manager.table_versions(&parsed.tables);
   manager.invalidate_tables(&parsed.tables);
   assert!(!manager.store_cached_query_versioned(
      "db:",
      &parsed.tables,
      parsed.fingerprint,
      &rs,
      60_000,
      &versions,
   ));
   assert!(
      manager
         .get_encoded("db:", "users", parsed.fingerprint)
         .is_none()
   );
   assert!(
      manager
         .get_cached_query("db:", "users", parsed.fingerprint)
         .is_none()
   );

   // A snapshot taken after the invalidation still stores normally.
   let versions = manager.table_versions(&parsed.tables);
   assert!(manager.store_cached_query_versioned(
      "db:",
      &parsed.tables,
      parsed.fingerprint,
      &rs,
      60_000,
      &versions,
   ));
   assert!(
      manager
         .get_encoded("db:", "users", parsed.fingerprint)
         .is_some()
   );
}

#[test]
fn test_index_rebuild_restores_invalidation() {
   let storage = Arc::new(MemoryStorage::default());
   let parsed = parse_sql("SELECT id FROM users WHERE id = 7;");
   let rs = DbResultSet {
      columns: vec![ColumnInfo {
         name: "id".to_string(),
         type_oid: 23,
      }],
      rows: vec![DbRow {
         values: vec![DbValue::Int(7)],
      }],
      rows_affected: 1,
   };

   {
      let manager = DbCacheManager::new(storage.clone());
      manager.store_cached_query("db:", &parsed.tables, parsed.fingerprint, &rs, 60_000);
      assert!(
         manager
            .get_cached_query("db:", "users", parsed.fingerprint)
            .is_some()
      );
   }

   // Fresh manager over the same backing store simulates a process restart.
   let manager = DbCacheManager::new(storage.clone());
   assert_eq!(manager.tracked_key_count(), 0);
   assert_eq!(manager.rebuild_tracker("db:"), 1);
   assert_eq!(manager.tracked_key_count(), 1);

   let evicted = manager.invalidate_tables(&["users".to_string()]);
   assert_eq!(evicted, 1);
   assert!(
      manager
         .get_cached_query("db:", "users", parsed.fingerprint)
         .is_none()
   );
   assert!(
      storage
         .get(&make_cache_key("db:", "users", parsed.fingerprint))
         .is_none()
   );
   // The persisted index marker is dropped along with the result.
   assert!(
      storage
         .get(&format!("db:idx:users:{:016x}", parsed.fingerprint))
         .is_none()
   );
}

#[test]
fn test_flush_all_clears_results_and_index() {
   let storage = Arc::new(MemoryStorage::default());
   let manager = DbCacheManager::new(storage.clone());
   let parsed = parse_sql("SELECT id FROM users WHERE id = 9;");
   let rs = DbResultSet {
      columns: vec![ColumnInfo {
         name: "id".to_string(),
         type_oid: 23,
      }],
      rows: vec![DbRow {
         values: vec![DbValue::Int(9)],
      }],
      rows_affected: 1,
   };

   manager.store_cached_query("db:", &parsed.tables, parsed.fingerprint, &rs, 60_000);
   assert!(manager.flush_all("db:") >= 2); // result + index marker
   assert_eq!(manager.tracked_key_count(), 0);
   assert!(
      manager
         .get_cached_query("db:", "users", parsed.fingerprint)
         .is_none()
   );
   assert!(
      storage
         .get(&make_cache_key("db:", "users", parsed.fingerprint))
         .is_none()
   );
}

#[test]
fn test_extended_protocol_message_parsing() {
   let mut buf = Vec::new();

   let mut parse_body = Vec::new();
   parse_body.extend_from_slice(b"stmt1\0");
   parse_body.extend_from_slice(b"SELECT * FROM users WHERE id = $1\0");
   parse_body.extend_from_slice(&1i16.to_be_bytes());
   parse_body.extend_from_slice(&23u32.to_be_bytes());
   put_message(&mut buf, b'P', &parse_body);

   buf.extend_from_slice(&encode_bind(
      "p1",
      "stmt1",
      &[0],
      &[Some(b"42".to_vec())],
      &[0],
   ));

   let mut describe_body = Vec::new();
   describe_body.push(b'P');
   describe_body.extend_from_slice(b"p1\0");
   put_message(&mut buf, b'D', &describe_body);

   let mut execute_body = Vec::new();
   execute_body.extend_from_slice(b"p1\0");
   execute_body.extend_from_slice(&10i32.to_be_bytes());
   put_message(&mut buf, b'E', &execute_body);

   put_message(&mut buf, b'S', &[]);
   put_message(&mut buf, b'H', &[]);
   put_message(&mut buf, b'X', &[]);

   let mut reader = FrameReader::new(Cursor::new(buf));
   assert_eq!(
      reader.read_frontend(false).unwrap(),
      FrontendMessage::Parse {
         name: "stmt1".to_string(),
         sql: "SELECT * FROM users WHERE id = $1".to_string(),
         param_oids: vec![23],
      }
   );
   assert_eq!(
      reader.read_frontend(false).unwrap(),
      FrontendMessage::Bind {
         portal: "p1".to_string(),
         statement: "stmt1".to_string(),
         param_formats: vec![0],
         params: vec![Some(b"42".to_vec())],
         result_formats: vec![0],
      }
   );
   assert_eq!(
      reader.read_frontend(false).unwrap(),
      FrontendMessage::Describe {
         kind: b'P',
         name: "p1".to_string(),
      }
   );
   assert_eq!(
      reader.read_frontend(false).unwrap(),
      FrontendMessage::Execute {
         portal: "p1".to_string(),
         max_rows: 10,
      }
   );
   assert_eq!(reader.read_frontend(false).unwrap(), FrontendMessage::Sync);
   assert_eq!(reader.read_frontend(false).unwrap(), FrontendMessage::Flush);
   assert_eq!(
      reader.read_frontend(false).unwrap(),
      FrontendMessage::Terminate
   );
}

#[test]
fn test_copy_merge_and_savepoint_classification() {
   let copy_from = parse_sql("COPY users (id, name) FROM STDIN");
   assert!(copy_from.writes);
   assert_eq!(copy_from.tables, vec!["users".to_string()]);

   let copy_query = parse_sql("COPY (SELECT id FROM users) TO STDOUT");
   assert!(!copy_query.writes);
   assert!(!copy_query.is_cacheable);

   let merge = parse_sql(
      "MERGE INTO users u USING staged s ON u.id = s.id \
       WHEN MATCHED THEN UPDATE SET name = s.name",
   );
   assert!(merge.writes);
   assert!(merge.tables.contains(&"users".to_string()));

   assert_eq!(parse_sql("ROLLBACK").kind, QueryKind::Rollback);
   // ROLLBACK TO SAVEPOINT keeps the transaction (and earlier writes) open.
   assert_eq!(
      parse_sql("ROLLBACK TO SAVEPOINT before_write").kind,
      QueryKind::Other
   );
}

fn text_row(values: &[&str]) -> DbRow {
   DbRow {
      values: values
         .iter()
         .map(|value| DbValue::Text(value.to_string()))
         .collect(),
   }
}

fn probe_result() -> DbResultSet {
   DbResultSet {
      columns: vec![ColumnInfo {
         name: "name".to_string(),
         type_oid: 25,
      }],
      rows: vec![DbRow {
         values: vec![DbValue::Text("alpha".to_string())],
      }],
      rows_affected: 1,
   }
}

#[test]
fn test_session_changes_and_catalog_metadata_are_extracted() {
   let set = parse_sql("SET search_path TO app, public");
   assert_eq!(set.kind, QueryKind::Set);
   assert_eq!(set.session_changes.len(), 1);
   assert!(matches!(
      set.session_changes[0],
      SessionChange::Set { local: false, .. }
   ));
   assert!(!set.is_cacheable);

   let local = parse_sql("SET LOCAL statement_timeout = '5s'");
   assert!(matches!(
      local.session_changes[0],
      SessionChange::Set { local: true, .. }
   ));

   let reset = parse_sql("RESET search_path");
   assert!(matches!(reset.session_changes[0], SessionChange::Reset));

   let discard = parse_sql("DISCARD ALL");
   assert!(matches!(
      discard.session_changes[0],
      SessionChange::DiscardAll
   ));

   // set_config(name, value, false) mutates the session through a function.
   let set_config = parse_sql("SELECT set_config('work_mem', '64MB', false)");
   assert!(set_config.mutates_session);

   let functional = parse_sql("SELECT my_stable_fn(), public.other_fn(id) FROM users");
   assert_eq!(
      functional.functions,
      vec!["my_stable_fn".to_string(), "public.other_fn".to_string()]
   );
   // The AST blocklist still rejects volatile functions without catalog data.
   assert!(!parse_sql("SELECT random() FROM users").is_cacheable);

   assert!(parse_sql("CREATE VIEW v AS SELECT id FROM users").schema_change);
   assert!(parse_sql("DROP TABLE users").schema_change);
   assert!(!parse_sql("SELECT id FROM users").schema_change);

   // Catalog relations are never shared between sessions.
   assert!(!parse_sql("SELECT * FROM pg_class").is_cacheable);
   assert!(!parse_sql("SELECT * FROM information_schema.tables").is_cacheable);
   assert!(parse_sql("SELECT * FROM public.users").is_cacheable);
}

#[test]
fn test_catalog_snapshot_marks_functions_and_expands_views() {
   let function_rows = vec![
      text_row(&["pg_catalog", "my_stable_fn"]),
      text_row(&["pg_catalog", "random"]),
   ];
   let view_rows = vec![
      text_row(&["user_view", "SELECT users.id FROM users", "public.users"]),
      text_row(&[
         "volatile_view",
         "SELECT random() AS r FROM users",
         "public.users",
      ]),
   ];
   let snapshot = CatalogSnapshot::from_rows(&function_rows, &view_rows);

   assert!(snapshot.is_volatile("my_stable_fn"));
   assert!(snapshot.is_volatile("pg_catalog.my_stable_fn"));
   assert!(!snapshot.is_volatile("lower"));
   assert!(snapshot.references_volatile_view(&["volatile_view".to_string()]));
   assert!(!snapshot.references_volatile_view(&["user_view".to_string()]));

   let expanded = snapshot.expand(&["user_view".to_string()]);
   assert!(expanded.contains(&"user_view".to_string()));
   assert!(expanded.contains(&"public.users".to_string()));
   assert!(expanded.contains(&"users".to_string()));

   let manager = DbCacheManager::new(Arc::new(MemoryStorage::default()));
   manager.install_catalog(snapshot);
   assert!(manager.catalog_ready());
   assert!(!manager.is_cacheable(&parse_sql(
      "SELECT id FROM users WHERE my_stable_fn(id) = 1"
   )));
   assert!(!manager.is_cacheable(&parse_sql("SELECT id FROM volatile_view")));
   assert!(manager.is_cacheable(&parse_sql("SELECT id FROM users WHERE id = 1")));
}

#[test]
fn test_external_write_detection_invalidates_changed_tables() {
   use std::sync::atomic::Ordering;

   let storage = Arc::new(MemoryStorage::default());
   let manager = DbCacheManager::new(storage);
   let parsed = parse_sql("SELECT id, name FROM users");
   let result = probe_result();
   manager.store_cached_query("db:", &parsed.tables, parsed.fingerprint, &result, 60_000);
   assert!(
      manager
         .get_cached_query("db:", "users", parsed.fingerprint)
         .is_some()
   );

   // The first observation only records a baseline.
   let baseline = vec![(
      "users".to_string(),
      TableCounters {
         inserts: 7,
         updates: 0,
         deletes: 0,
      },
   )];
   assert!(manager.apply_external_counters(baseline).is_empty());
   assert!(
      manager
         .get_cached_query("db:", "users", parsed.fingerprint)
         .is_some()
   );

   // A counter change means another backend wrote to the table.
   let changed = vec![(
      "users".to_string(),
      TableCounters {
         inserts: 7,
         updates: 1,
         deletes: 0,
      },
   )];
   let changed_tables = manager.apply_external_counters(changed);
   assert_eq!(changed_tables, vec!["users".to_string()]);
   assert!(
      manager
         .get_cached_query("db:", "users", parsed.fingerprint)
         .is_none()
   );
   assert_eq!(
      manager
         .metrics()
         .external_invalidations
         .load(Ordering::Relaxed),
      1
   );
}

#[test]
fn test_tracked_table_names_reflects_active_cache() {
   let storage = Arc::new(MemoryStorage::default());
   let manager = DbCacheManager::new(storage);
   assert!(manager.tracked_table_names().is_empty());

   let parsed = parse_sql("SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id");
   let result = probe_result();
   manager.store_cached_query("db:", &parsed.tables, parsed.fingerprint, &result, 60_000);

   let tables = manager.tracked_table_names();
   assert_eq!(tables, vec!["orders".to_string(), "users".to_string()]);

   manager.invalidate_tables(&["users".to_string()]);
   assert!(manager.tracked_table_names().is_empty());
}

#[test]
fn test_capacity_limit_evicts_tracked_entries() {
   use std::sync::atomic::Ordering;

   let storage = Arc::new(MemoryStorage::default());
   let manager = DbCacheManager::with_limits(
      storage,
      CacheLimits {
         max_keys: 2,
         ttl_jitter_pct: 0,
         sync_interval_ms: 0,
      },
   );
   let result = probe_result();
   for index in 0..3 {
      let parsed = parse_sql(&format!("SELECT id FROM t{index}"));
      manager.store_cached_query("db:", &parsed.tables, parsed.fingerprint, &result, 60_000);
   }

   assert_eq!(manager.tracked_key_count(), 2);
   assert_eq!(
      manager.metrics().capacity_evictions.load(Ordering::Relaxed),
      1
   );
}

#[test]
fn test_single_flight_coalesces_fillers() {
   let manager = Arc::new(DbCacheManager::new(Arc::new(MemoryStorage::default())));
   let guard = manager.begin_fill("db:users:abc").expect("first loader");

   // A second loader must not start while the first is in flight.
   assert!(manager.begin_fill("db:users:abc").is_none());

   let waiter = Arc::clone(&manager);
   let handle = std::thread::spawn(move || waiter.wait_for_fill("db:users:abc"));
   std::thread::sleep(std::time::Duration::from_millis(20));
   drop(guard);
   assert!(handle.join().expect("waiter thread"));

   // Once the leader finishes, the slot is reusable.
   assert!(manager.begin_fill("db:users:abc").is_some());
}

#[test]
fn test_stats_report_counters_and_gauges() {
   use std::collections::HashMap;

   let manager = DbCacheManager::new(Arc::new(MemoryStorage::default()));
   let parsed = parse_sql("SELECT id, name FROM users");
   let result = probe_result();
   manager.store_cached_query("db:", &parsed.tables, parsed.fingerprint, &result, 60_000);
   assert!(
      manager
         .get_encoded("db:", "users", parsed.fingerprint)
         .is_some()
   );

   let stats: HashMap<&str, u64> = manager.stats().into_iter().collect();
   assert_eq!(stats["fills"], 1);
   assert_eq!(stats["hits"], 1);
   assert_eq!(stats["tracked_keys"], 1);
   assert_eq!(stats["encoded_entries"], 1);
   assert_eq!(stats["max_keys"], 100_000);
}

#[cfg(unix)]
fn wait_until(timeout: std::time::Duration, mut condition: impl FnMut() -> bool) -> bool {
   let deadline = std::time::Instant::now() + timeout;
   while std::time::Instant::now() < deadline {
      if condition() {
         return true;
      }
      std::thread::sleep(std::time::Duration::from_millis(20));
   }
   condition()
}

#[cfg(unix)]
#[test]
fn test_bus_propagates_invalidations_between_managers() {
   use std::sync::atomic::Ordering;
   use std::time::{Duration, SystemTime, UNIX_EPOCH};

   let nanos = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_nanos();
   let path = std::env::temp_dir().join(format!("fyrodb-bus-{}-{nanos}.sock", std::process::id()));
   let path = path.to_string_lossy().to_string();

   let manager_a = Arc::new(DbCacheManager::new(Arc::new(MemoryStorage::default())));
   let manager_b = Arc::new(DbCacheManager::new(Arc::new(MemoryStorage::default())));
   let bus_a = fyro_dbcache::bus::start(&path, Arc::clone(&manager_a), "db:".to_string())
      .expect("start bus a");
   let bus_b = fyro_dbcache::bus::start(&path, Arc::clone(&manager_b), "db:".to_string())
      .expect("start bus b");
   assert!(wait_until(Duration::from_secs(5), || {
      manager_a.metrics().bus_connected.load(Ordering::Relaxed)
         && manager_b.metrics().bus_connected.load(Ordering::Relaxed)
   }));

   // Entries stored after both peers joined are invalidated across processes.
   let parsed = parse_sql("SELECT id, name FROM users");
   manager_b.store_cached_query(
      "db:",
      &parsed.tables,
      parsed.fingerprint,
      &probe_result(),
      60_000,
   );
   assert!(
      manager_b
         .get_cached_query("db:", "users", parsed.fingerprint)
         .is_some()
   );

   bus_a.invalidate(&["users".to_string()]);
   assert!(wait_until(Duration::from_secs(5), || {
      manager_b
         .get_cached_query("db:", "users", parsed.fingerprint)
         .is_none()
   }));

   // A flush broadcast clears unrelated local entries too.
   let orders = parse_sql("SELECT id FROM orders");
   manager_b.store_cached_query(
      "db:",
      &orders.tables,
      orders.fingerprint,
      &probe_result(),
      60_000,
   );
   bus_a.flush();
   assert!(wait_until(Duration::from_secs(5), || {
      manager_b
         .get_cached_query("db:", "orders", orders.fingerprint)
         .is_none()
   }));
   assert!(
      manager_b
         .metrics()
         .bus_messages_received
         .load(Ordering::Relaxed)
         >= 2
   );

   // A manager that joins with persisted keys flushes them: the broker may
   // have relayed invalidations while this process was not listening.
   let late = Arc::new(DbCacheManager::new(Arc::new(MemoryStorage::default())));
   let late_parsed = parse_sql("SELECT id FROM accounts");
   late.store_cached_query(
      "db:",
      &late_parsed.tables,
      late_parsed.fingerprint,
      &probe_result(),
      60_000,
   );
   let _late_bus = fyro_dbcache::bus::start(&path, Arc::clone(&late), "db:".to_string())
      .expect("start late bus");
   assert!(wait_until(Duration::from_secs(5), || {
      late
         .get_cached_query("db:", "accounts", late_parsed.fingerprint)
         .is_none()
   }));

   // Keep the publishers alive until the assertions complete.
   drop((bus_a, bus_b));
}

// ── ParsedQueryCache deep tests ───────────────────────────────────────────────

#[test]
fn test_parse_cache_same_arc_on_repeated_calls() {
   let cache = ParsedQueryCache::default();
   let sql = "SELECT id, name FROM customers WHERE id = 1;";
   let first = cache.get_or_parse(sql);
   let second = cache.get_or_parse(sql);
   // Repeated calls must return the same Arc (no extra parse).
   assert!(Arc::ptr_eq(&first, &second));
   assert_eq!(cache.len(), 1);
}

#[test]
fn test_parse_cache_distinct_sqls_produce_distinct_arcs() {
   let cache = ParsedQueryCache::default();
   let a = cache.get_or_parse("SELECT 1;");
   let b = cache.get_or_parse("SELECT 2;");
   assert!(!Arc::ptr_eq(&a, &b));
   assert_eq!(cache.len(), 2);
}

#[test]
fn test_parse_cache_concurrent_hits_return_consistent_result() {
   use std::thread;
   let cache = Arc::new(ParsedQueryCache::default());
   let sql = "SELECT id, email FROM accounts WHERE id = $1;";
   // Pre-seed so the first writer wins; all concurrent readers see the same Arc.
   let expected = cache.get_or_parse(sql);

   let mut handles = Vec::new();
   for _ in 0..16 {
      let c = Arc::clone(&cache);
      handles.push(thread::spawn(move || c.get_or_parse(sql)));
   }
   for h in handles {
      let got = h.join().unwrap();
      assert_eq!(got.fingerprint, expected.fingerprint);
      assert_eq!(got.tables, expected.tables);
   }
}

#[test]
fn test_parse_cache_manager_analyze_caches_across_calls() {
   let storage = Arc::new(MemoryStorage::default());
   let manager = DbCacheManager::new(storage);
   let sql = "SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = 5;";

   assert_eq!(manager.parsed_cache_len(), 0);
   let a = manager.analyze(sql);
   assert_eq!(manager.parsed_cache_len(), 1);
   let b = manager.analyze(sql);
   // Same entry reused — cache length does not grow.
   assert_eq!(manager.parsed_cache_len(), 1);
   assert!(Arc::ptr_eq(&a, &b));
}

#[test]
fn test_parse_cache_fingerprint_is_stable() {
   let cache = ParsedQueryCache::default();
   let sql = "SELECT id FROM widgets WHERE active = true;";
   let fp1 = cache.get_or_parse(sql).fingerprint;
   let fp2 = cache.get_or_parse(sql).fingerprint;
   let fp3 = ParsedQueryCache::default().get_or_parse(sql).fingerprint;
   assert_eq!(fp1, fp2);
   assert_eq!(fp1, fp3);
}

#[test]
fn test_analyze_bound_empty_params_equals_base_fingerprint() {
   let storage = Arc::new(MemoryStorage::default());
   let manager = DbCacheManager::new(storage);
   let sql = "SELECT id FROM widgets WHERE id = 1;";
   let (parsed, fp) = manager.analyze_bound(sql, &[], &[]);
   assert_eq!(fp, parsed.fingerprint);
}

#[test]
fn test_analyze_bound_different_params_produce_different_fingerprints() {
   let storage = Arc::new(MemoryStorage::default());
   let manager = DbCacheManager::new(storage);
   let sql = "SELECT id FROM users WHERE id = $1;";
   let p42 = Some(b"42".to_vec());
   let p99 = Some(b"99".to_vec());
   let (_, fp42) = manager.analyze_bound(sql, &[p42], &[]);
   let (_, fp99) = manager.analyze_bound(sql, &[p99], &[]);
   assert_ne!(fp42, fp99);
}

// ── Encoded cache deep tests ──────────────────────────────────────────────────

#[test]
fn test_get_encoded_returns_none_on_miss() {
   let storage = Arc::new(MemoryStorage::default());
   let manager = DbCacheManager::new(storage);
   let result = manager.get_encoded("db:", "users", 0xdeadbeef_u64);
   assert!(result.is_none());
}

#[test]
fn test_get_encoded_returns_wire_bytes_after_store() {
   let storage = Arc::new(MemoryStorage::default());
   let manager = DbCacheManager::new(storage);
   let sql = "SELECT id, username FROM users WHERE id = 1;";
   let parsed = parse_sql(sql);
   let rs = make_one_row_result();
   manager.store_cached_query("db:", &parsed.tables, parsed.fingerprint, &rs, 60_000);

   let entry = manager
      .get_encoded("db:", "users", parsed.fingerprint)
      .expect("expected encoded entry");
   let mut buf = Vec::new();
   entry.write_simple_response(&mut buf);
   // A valid response must start with RowDescription ('T').
   assert_eq!(buf[0], b'T', "response must start with RowDescription");
   // ReadyForQuery message is [b'Z', 0, 0, 0, 5, status]; find it in the tail.
   // The last byte is the transaction-status byte ('I', 'T', or 'E'), not 'Z'.
   assert!(
      buf.len() >= 6 && buf[buf.len() - 6] == b'Z',
      "response must contain ReadyForQuery ('Z') message; last 6 bytes: {:?}",
      &buf[buf.len().saturating_sub(6)..]
   );
}

#[test]
fn test_store_overwrite_replaces_entry() {
   let storage = Arc::new(MemoryStorage::default());
   let manager = DbCacheManager::new(storage);
   let sql = "SELECT id FROM widgets WHERE id = 1;";
   let parsed = parse_sql(sql);

   let rs1 = DbResultSet {
      columns: vec![ColumnInfo { name: "id".to_string(), type_oid: 23 }],
      rows: vec![DbRow { values: vec![DbValue::Int(1)] }],
      rows_affected: 1,
   };
   let rs2 = DbResultSet {
      columns: vec![ColumnInfo { name: "id".to_string(), type_oid: 23 }],
      rows: vec![DbRow { values: vec![DbValue::Int(999)] }],
      rows_affected: 1,
   };

   manager.store_cached_query("db:", &parsed.tables, parsed.fingerprint, &rs1, 60_000);
   manager.store_cached_query("db:", &parsed.tables, parsed.fingerprint, &rs2, 60_000);

   // Should return the second (overwritten) entry.
   let entry = manager
      .get_encoded("db:", "widgets", parsed.fingerprint)
      .expect("entry should exist");
   let mut buf = Vec::new();
   entry.write_simple_response(&mut buf);
   // DataRow body should contain "999".
   let response_str = String::from_utf8_lossy(&buf);
   assert!(response_str.contains("999"));
}

#[test]
fn test_encoded_cache_concurrent_readers() {
   use std::thread;
   let storage = Arc::new(MemoryStorage::default());
   let manager = Arc::new(DbCacheManager::new(storage));
   let sql = "SELECT id, status FROM orders WHERE id = 7;";
   let parsed = parse_sql(sql);
   manager.store_cached_query(
      "db:",
      &parsed.tables,
      parsed.fingerprint,
      &make_one_row_result(),
      60_000,
   );

   let mut handles = Vec::new();
   for _ in 0..16 {
      let mgr = Arc::clone(&manager);
      let fp = parsed.fingerprint;
      handles.push(thread::spawn(move || {
         for _ in 0..1_000 {
            let r = mgr.get_encoded("db:", "orders", fp);
            assert!(r.is_some());
         }
      }));
   }
   for h in handles {
      h.join().unwrap();
   }
}

// ── Cache invalidation deep tests ─────────────────────────────────────────────

#[test]
fn test_invalidate_single_table_leaves_others_intact() {
   let storage = Arc::new(MemoryStorage::default());
   let manager = DbCacheManager::new(storage);

   let users_sql = "SELECT id FROM users WHERE id = 1;";
   let orders_sql = "SELECT id FROM orders WHERE id = 1;";
   let up = parse_sql(users_sql);
   let op = parse_sql(orders_sql);
   let rs = make_one_row_result();
   manager.store_cached_query("db:", &up.tables, up.fingerprint, &rs, 60_000);
   manager.store_cached_query("db:", &op.tables, op.fingerprint, &rs, 60_000);

   let evicted = manager.invalidate_tables(&["users".to_string()]);
   assert_eq!(evicted, 1);
   assert!(manager.get_cached_query("db:", "users", up.fingerprint).is_none());
   assert!(manager.get_cached_query("db:", "orders", op.fingerprint).is_some());
}

#[test]
fn test_invalidate_joined_query_when_any_constituent_table_is_written() {
   let storage = Arc::new(MemoryStorage::default());
   let manager = DbCacheManager::new(storage);

   let join_sql = "SELECT u.id, p.title FROM users u JOIN posts p ON u.id = p.author_id;";
   let parsed = parse_sql(join_sql);
   assert!(parsed.tables.contains(&"users".to_string()));
   assert!(parsed.tables.contains(&"posts".to_string()));

   manager.store_cached_query("db:", &parsed.tables, parsed.fingerprint, &make_one_row_result(), 60_000);

   // Write to 'posts' → should invalidate the JOIN entry.
   let evicted = manager.invalidate_tables(&["posts".to_string()]);
   assert_eq!(evicted, 1);
   assert!(manager.get_cached_query("db:", "users", parsed.fingerprint).is_none());
}

#[test]
fn test_flush_all_clears_encoded_and_json_caches() {
   let storage = Arc::new(MemoryStorage::default());
   let manager = DbCacheManager::new(storage);

   for i in 0..10u64 {
      let rs = make_one_row_result();
      manager.store_cached_query("db:", &["t1".to_string()], i, &rs, 60_000);
   }
   assert!(manager.get_encoded("db:", "t1", 0).is_some());

   manager.flush_all("db:");
   for i in 0..10u64 {
      assert!(manager.get_encoded("db:", "t1", i).is_none());
   }
}

// ── Fingerprint correctness ───────────────────────────────────────────────────

#[test]
fn test_fingerprint_normalizes_whitespace() {
   // Normalize collapses multiple spaces between tokens and lowercases keywords.
   // Note: whitespace immediately before ';' is preserved by the normalizer,
   // so the comparison pairs must agree on semicolon placement.
   let a = parse_sql("SELECT id FROM users WHERE id = 1;");
   let b = parse_sql("  SELECT   id  FROM  users  WHERE  id  =  1;  ");
   assert_eq!(a.fingerprint, b.fingerprint, "normalized forms must match");
}

#[test]
fn test_fingerprint_normalizes_keyword_case() {
   let a = parse_sql("SELECT id FROM users WHERE id = 1;");
   let b = parse_sql("select id from users where id = 1;");
   assert_eq!(a.fingerprint, b.fingerprint);
}

#[test]
fn test_fingerprint_distinguishes_different_tables() {
   let a = parse_sql("SELECT id FROM users WHERE id = 1;");
   let b = parse_sql("SELECT id FROM orders WHERE id = 1;");
   assert_ne!(a.fingerprint, b.fingerprint);
}

#[test]
fn test_parameterized_fingerprint_varies_with_param_value() {
   let sql = "SELECT id FROM users WHERE id = $1;";
   let parsed = parse_sql(sql);
   let p42 = Some(b"42".to_vec());
   let p43 = Some(b"43".to_vec());
   let fp42 = compute_fingerprint_with_params(&parsed.normalized, std::slice::from_ref(&p42), &[]);
   let fp43 = compute_fingerprint_with_params(&parsed.normalized, std::slice::from_ref(&p43), &[]);
   assert_ne!(fp42, fp43);
}

#[test]
fn test_parameterized_fingerprint_stable_across_calls() {
   let sql = "SELECT id FROM users WHERE id = $1;";
   let parsed = parse_sql(sql);
   let param = Some(b"42".to_vec());
   let fp1 = compute_fingerprint_with_params(&parsed.normalized, std::slice::from_ref(&param), &[]);
   let fp2 = compute_fingerprint_with_params(&parsed.normalized, std::slice::from_ref(&param), &[]);
   assert_eq!(fp1, fp2);
}

// ── analyze() hot-path throughput ────────────────────────────────────────────

#[test]
fn test_analyze_cache_hot_path_completes_quickly() {
   // 500k analyze() calls on the same SQL should complete in well under 2s
   // even on slow CI — this guards against accidental lock contention regressions.
   use std::time::Instant;
   let storage = Arc::new(MemoryStorage::default());
   let manager = DbCacheManager::new(storage);
   let sql = "SELECT id, username, email FROM users WHERE id = 42;";
   manager.analyze(sql); // warm the cache

   let start = Instant::now();
   for _ in 0..500_000 {
      std::hint::black_box(manager.analyze(sql));
   }
   let elapsed = start.elapsed();
   assert!(
      elapsed.as_secs() < 2,
      "analyze() cache hit took {}ms for 500k calls — too slow",
      elapsed.as_millis()
   );
}

#[test]
fn test_encoded_cache_hot_path_completes_quickly() {
   use std::time::Instant;
   let storage = Arc::new(MemoryStorage::default());
   let manager = DbCacheManager::new(storage);
   let sql = "SELECT id, username FROM users WHERE id = 1;";
   let parsed = parse_sql(sql);
   manager.store_cached_query("db:", &parsed.tables, parsed.fingerprint, &make_one_row_result(), 60_000);
   let fp = parsed.fingerprint;

   let start = Instant::now();
   for _ in 0..500_000 {
      std::hint::black_box(manager.get_encoded("db:", "users", fp));
   }
   let elapsed = start.elapsed();
   assert!(
      elapsed.as_secs() < 2,
      "get_encoded() cache hit took {}ms for 500k calls — too slow",
      elapsed.as_millis()
   );
}

// ── Utility ───────────────────────────────────────────────────────────────────

fn make_one_row_result() -> DbResultSet {
   DbResultSet {
      columns: vec![
         ColumnInfo { name: "id".to_string(), type_oid: 23 },
         ColumnInfo { name: "name".to_string(), type_oid: 25 },
      ],
      rows: vec![DbRow {
         values: vec![DbValue::Int(42), DbValue::Text("test".to_string())],
      }],
      rows_affected: 1,
   }
}
