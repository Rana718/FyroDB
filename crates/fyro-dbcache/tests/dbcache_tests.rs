use fyro_dbcache::protocol::{
   FrameReader, FrontendMessage, encode_bind, put_message, read_frontend_message, write_result_set,
};
use fyro_dbcache::query::{QueryKind, compute_fingerprint_with_params, make_cache_key, parse_sql};
use fyro_dbcache::row::{ColumnInfo, DbResultSet, DbRow, DbValue};
use fyro_dbcache::{
   CacheLimits, CatalogSnapshot, DbCacheManager, DbCacheStorage, MemoryStorage, SessionChange,
   TableCounters,
};
use std::io::Cursor;
use std::sync::Arc;

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
   assert!(manager.is_cacheable(&parse_sql(
      "SELECT id FROM users WHERE id = 1"
   )));
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
   assert_eq!(manager.apply_external_counters(baseline), 0);
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
   assert_eq!(manager.apply_external_counters(changed), 1);
   assert!(
      manager
         .get_cached_query("db:", "users", parsed.fingerprint)
         .is_none()
   );
   assert_eq!(
      manager.metrics().external_invalidations.load(Ordering::Relaxed),
      1
   );
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
