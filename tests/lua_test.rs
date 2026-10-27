use fyro_db::commands;
use fyro_db::storage::store::Store;
use std::sync::Arc;

fn run_cmd(store: &Arc<Store>, parts: &[&str]) -> String {
   let mut out = Vec::new();
   commands::execute(parts, store, &mut out);
   String::from_utf8(out).expect("valid utf-8 response")
}

#[test]
fn test_lua_eval_basic_returns() {
   let store = Arc::new(Store::new());

   // String
   assert_eq!(
      run_cmd(&store, &["EVAL", "return 'hello world'", "0"]),
      "$11\r\nhello world\r\n"
   );

   // Integer
   assert_eq!(
      run_cmd(&store, &["EVAL", "return 42", "0"]),
      ":42\r\n"
   );

   // Float truncated
   assert_eq!(
      run_cmd(&store, &["EVAL", "return 42.9", "0"]),
      ":42\r\n"
   );

   // Boolean true -> 1
   assert_eq!(
      run_cmd(&store, &["EVAL", "return true", "0"]),
      ":1\r\n"
   );

   // Boolean false -> nil ($-1\r\n)
   assert_eq!(
      run_cmd(&store, &["EVAL", "return false", "0"]),
      "$-1\r\n"
   );

   // Nil -> nil ($-1\r\n)
   assert_eq!(
      run_cmd(&store, &["EVAL", "return nil", "0"]),
      "$-1\r\n"
   );

   // Array table
   assert_eq!(
      run_cmd(&store, &["EVAL", "return {'first', 100, false}", "0"]),
      "*3\r\n$5\r\nfirst\r\n:100\r\n$-1\r\n"
   );
}

#[test]
fn test_lua_keys_and_argv() {
   let store = Arc::new(Store::new());

   let script = "return {KEYS[1], KEYS[2], ARGV[1], ARGV[2]}";
   let res = run_cmd(&store, &["EVAL", script, "2", "k1", "k2", "a1", "a2"]);
   assert_eq!(res, "*4\r\n$2\r\nk1\r\n$2\r\nk2\r\n$2\r\na1\r\n$2\r\na2\r\n");
}

#[test]
fn test_lua_redis_call_set_get_incr() {
   let store = Arc::new(Store::new());

   // SET and GET via redis.call
   let script = r#"
      redis.call('SET', KEYS[1], ARGV[1])
      return redis.call('GET', KEYS[1])
   "#;
   assert_eq!(
      run_cmd(&store, &["EVAL", script, "1", "user:1", "alice"]),
      "$5\r\nalice\r\n"
   );

   // INCR
   let script_incr = r#"
      redis.call('SET', KEYS[1], ARGV[1])
      redis.call('INCR', KEYS[1])
      return redis.call('GET', KEYS[1])
   "#;
   assert_eq!(
      run_cmd(&store, &["EVAL", script_incr, "1", "counter", "10"]),
      "$2\r\n11\r\n"
   );
}

#[test]
fn test_lua_redis_call_error_propagation() {
   let store = Arc::new(Store::new());

   // Wrong type error in redis.call should fail the script
   let script = r#"
      redis.call('SET', KEYS[1], 'not_a_hash')
      return redis.call('HGET', KEYS[1], 'field')
   "#;
   let res = run_cmd(&store, &["EVAL", script, "1", "str_key"]);
   assert!(res.starts_with("-ERR Error running script:"));
   assert!(res.contains("WRONGTYPE"));
}

#[test]
fn test_lua_redis_pcall() {
   let store = Arc::new(Store::new());

   // pcall catches error and returns {err = msg}
   let script = r#"
      redis.call('SET', KEYS[1], 'not_a_hash')
      local res = redis.pcall('HGET', KEYS[1], 'field')
      if type(res) == 'table' and res.err ~= nil then
         return "caught_error"
      else
         return "no_error"
      end
   "#;
   assert_eq!(
      run_cmd(&store, &["EVAL", script, "1", "pcall_key"]),
      "$12\r\ncaught_error\r\n"
   );
}

#[test]
fn test_lua_status_and_error_replies() {
   let store = Arc::new(Store::new());

   // status_reply
   assert_eq!(
      run_cmd(&store, &["EVAL", "return redis.status_reply('PONG')", "0"]),
      "+PONG\r\n"
   );

   // error_reply
   assert_eq!(
      run_cmd(&store, &["EVAL", "return redis.error_reply('Custom Error')", "0"]),
      "-ERR Custom Error\r\n"
   );
}

#[test]
fn test_lua_sha1hex() {
   let store = Arc::new(Store::new());

   // Standard SHA1 of "hello world" is 2aae6c35c94fcfb415dbe95f408b9ce91ee846ed
   assert_eq!(
      run_cmd(&store, &["EVAL", "return redis.sha1hex('hello world')", "0"]),
      "$40\r\n2aae6c35c94fcfb415dbe95f408b9ce91ee846ed\r\n"
   );
}

#[test]
fn test_lua_cjson() {
   let store = Arc::new(Store::new());

   // cjson.decode and encode
   let script = r#"
      local doc = cjson.decode('{"name":"rana","score":100}')
      return {doc.name, doc.score}
   "#;
   assert_eq!(
      run_cmd(&store, &["EVAL", script, "0"]),
      "*2\r\n$4\r\nrana\r\n:100\r\n"
   );

   let script_enc = r#"
      local t = { count = 42 }
      return cjson.encode(t)
   "#;
   assert_eq!(
      run_cmd(&store, &["EVAL", script_enc, "0"]),
      "$12\r\n{\"count\":42}\r\n"
   );
}

#[test]
fn test_lua_bit_ops() {
   let store = Arc::new(Store::new());

   assert_eq!(
      run_cmd(&store, &["EVAL", "return bit.band(6, 3)", "0"]),
      ":2\r\n"
   );
   assert_eq!(
      run_cmd(&store, &["EVAL", "return bit.bor(4, 1)", "0"]),
      ":5\r\n"
   );
}

#[test]
fn test_script_load_exists_flush_evalsha() {
   let store = Arc::new(Store::new());

   let script = "return tonumber(ARGV[1]) * 2";

   // 1. SCRIPT LOAD
   let load_res = run_cmd(&store, &["SCRIPT", "LOAD", script]);
   assert!(load_res.starts_with("$40\r\n"));
   let sha1 = load_res.trim().lines().nth(1).unwrap().trim();

   // 2. SCRIPT EXISTS
   let exists_res = run_cmd(&store, &["SCRIPT", "EXISTS", sha1, "0000000000000000000000000000000000000000"]);
   assert_eq!(exists_res, "*2\r\n:1\r\n:0\r\n");

   // 3. EVALSHA
   let evalsha_res = run_cmd(&store, &["EVALSHA", sha1, "0", "21"]);
   assert_eq!(evalsha_res, ":42\r\n");

   // 4. SCRIPT FLUSH
   let flush_res = run_cmd(&store, &["SCRIPT", "FLUSH"]);
   assert_eq!(flush_res, "+OK\r\n");

   // 5. EVALSHA after flush returns NOSCRIPT
   let no_script_res = run_cmd(&store, &["EVALSHA", sha1, "0", "21"]);
   assert_eq!(no_script_res, "-NOSCRIPT No matching script. Please use EVAL.\r\n");
}

#[test]
fn test_lua_nested_eval_rejected() {
   let store = Arc::new(Store::new());

   let script = r#"
      return redis.call('EVAL', 'return 1', 0)
   "#;
   let res = run_cmd(&store, &["EVAL", script, "0"]);
   assert!(res.contains("This Redis command is not allowed from scripts"));
}

#[test]
fn test_lua_rate_limiter_and_conditional_cas() {
   let store = Arc::new(Store::new());

   // 1. Rate limiter pattern: INCR + EXPIRE on first hit
   let rate_limiter = r#"
      local current = redis.call('INCR', KEYS[1])
      if current == 1 then
         redis.call('EXPIRE', KEYS[1], ARGV[1])
      end
      return current
   "#;

   assert_eq!(
      run_cmd(&store, &["EVAL", rate_limiter, "1", "rate:user_123", "60"]),
      ":1\r\n"
   );
   assert_eq!(
      run_cmd(&store, &["EVAL", rate_limiter, "1", "rate:user_123", "60"]),
      ":2\r\n"
   );

   // 2. Compare-and-swap pattern (CAS)
   let cas = r#"
      local old = redis.call('GET', KEYS[1])
      if old == ARGV[1] then
         redis.call('SET', KEYS[1], ARGV[2])
         return 1
      else
         return 0
      end
   "#;

   run_cmd(&store, &["SET", "cas_key", "version1"]);

   // Wrong expected value -> returns 0, no update
   assert_eq!(
      run_cmd(&store, &["EVAL", cas, "1", "cas_key", "wrong_val", "version2"]),
      ":0\r\n"
   );
   assert_eq!(run_cmd(&store, &["GET", "cas_key"]), "$8\r\nversion1\r\n");

   // Correct expected value -> returns 1, updates
   assert_eq!(
      run_cmd(&store, &["EVAL", cas, "1", "cas_key", "version1", "version2"]),
      ":1\r\n"
   );
   assert_eq!(run_cmd(&store, &["GET", "cas_key"]), "$8\r\nversion2\r\n");
}

#[test]
fn test_lua_precompiled_bytecode_reuse_and_throughput() {
   let store = Arc::new(Store::new());

   let script = "return redis.call('INCR', KEYS[1])";
   let load_res = run_cmd(&store, &["SCRIPT", "LOAD", script]);
   let sha = load_res.trim_start_matches("$40\r\n").trim_end_matches("\r\n");

   let start = std::time::Instant::now();
   for i in 1..=5_000 {
      let res = run_cmd(&store, &["EVALSHA", sha, "1", "loop_counter"]);
      assert_eq!(res, format!(":{i}\r\n"));
   }
   let elapsed = start.elapsed();
   // 5,000 in-memory Redis calls inside Lua scripts should execute under 250ms
   assert!(elapsed.as_millis() < 250, "5,000 iterations took {:?}", elapsed);
}

#[test]
fn test_lua_uppercase_sha1_and_normalization() {
   let store = Arc::new(Store::new());

   let script = "return 999";
   let load_res = run_cmd(&store, &["SCRIPT", "LOAD", script]);
   let sha = load_res.trim_start_matches("$40\r\n").trim_end_matches("\r\n");
   let sha_upper = sha.to_ascii_uppercase();

   // SCRIPT EXISTS with uppercase SHA
   assert_eq!(
      run_cmd(&store, &["SCRIPT", "EXISTS", &sha_upper]),
      "*1\r\n:1\r\n"
   );

   // EVALSHA with uppercase SHA
   assert_eq!(
      run_cmd(&store, &["EVALSHA", &sha_upper, "0"]),
      ":999\r\n"
   );
}

#[test]
fn test_lua_script_flush_epoch_invalidation() {
   let store = Arc::new(Store::new());

   let script = "return 'epoch_test'";
   let sha_res = run_cmd(&store, &["SCRIPT", "LOAD", script]);
   let sha = sha_res.trim_start_matches("$40\r\n").trim_end_matches("\r\n");

   assert_eq!(run_cmd(&store, &["EVALSHA", sha, "0"]), "$10\r\nepoch_test\r\n");

   // SCRIPT FLUSH
   assert_eq!(run_cmd(&store, &["SCRIPT", "FLUSH"]), "+OK\r\n");

   // EVALSHA must now fail with NOSCRIPT
   assert_eq!(
      run_cmd(&store, &["EVALSHA", sha, "0"]),
      "-NOSCRIPT No matching script. Please use EVAL.\r\n"
   );
}

#[test]
fn test_non_scripting_paths_unaffected() {
   let store = Arc::new(Store::new());

   // Set normal keys before script
   assert_eq!(run_cmd(&store, &["SET", "normal_k1", "val1"]), "+OK\r\n");
   assert_eq!(run_cmd(&store, &["SET", "normal_k2", "val2"]), "+OK\r\n");

   // Run complex script
   let script = r#"
      redis.call('SET', 'script_k', 'from_script')
      return redis.call('GET', KEYS[1])
   "#;
   assert_eq!(
      run_cmd(&store, &["EVAL", script, "1", "normal_k1"]),
      "$4\r\nval1\r\n"
   );

   // Normal paths after script continue to function with zero interference
   assert_eq!(run_cmd(&store, &["GET", "normal_k1"]), "$4\r\nval1\r\n");
   assert_eq!(run_cmd(&store, &["GET", "normal_k2"]), "$4\r\nval2\r\n");
   assert_eq!(run_cmd(&store, &["GET", "script_k"]), "$11\r\nfrom_script\r\n");
   assert_eq!(run_cmd(&store, &["MGET", "normal_k1", "normal_k2"]), "*2\r\n$4\r\nval1\r\n$4\r\nval2\r\n");
   assert_eq!(run_cmd(&store, &["PING"]), "+PONG\r\n");
}
