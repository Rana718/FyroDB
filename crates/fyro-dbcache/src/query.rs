use foldhash::fast::FixedState;
use parking_lot::RwLock;
use sqlparser::ast::{
   ContextModifier, CopySource, DiscardObject, Expr, ObjectName, ObjectNamePart, Query, Set,
   SetExpr, Statement, Visit, Visitor,
};
use sqlparser::dialect::{GenericDialect, PostgreSqlDialect};
use sqlparser::parser::Parser;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hasher};
use std::ops::ControlFlow;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryKind {
   Select,
   Insert,
   Update,
   Delete,
   Begin,
   Commit,
   Rollback,
   Set,
   Ddl,
   Other,
}

// A statement that changes per-session state which can affect query results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionChange {
   // Hashes the normalized statement into the session context fingerprint.
   Set { statement: String, local: bool },
   // The previous state cannot be reconstructed, so the proxy re-probes the
   // upstream connection before caching again.
   Reset,
   // DISCARD ALL drops prepared statements and resets every setting.
   DiscardAll,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedQuery {
   pub kind: QueryKind,
   pub tables: Vec<String>,
   pub normalized: String,
   pub fingerprint: u64,
   pub is_cacheable: bool,
   // True when any statement in the text can modify data or schema. A
   // multi-statement simple query may start with a SELECT and still write.
   pub writes: bool,
   // Function names referenced by the text, lowercased and deduplicated.
   pub functions: Vec<String>,
   // Any statement in the text changes session-resolved settings.
   pub session_changes: Vec<SessionChange>,
   // A statement in the text can change schema or function volatility, so the
   // catalog snapshot must be refreshed before the next fill.
   pub schema_change: bool,
   // A function call can alter session state (set_config); the context must be
   // re-probed after the statement runs.
   pub mutates_session: bool,
   // True when is_cacheable && functions.is_empty(): the catalog snapshot
   // cannot affect this query's cacheability, so the RwLock read is skipped.
   pub catalog_independent: bool,
}

// AST visitor collecting table references, CTE definitions, and volatile functions.
struct AstTableExtractor {
   tables: Vec<String>,
   functions: Vec<String>,
   seen_tables: HashSet<String, FixedState>,
   seen_functions: HashSet<String, FixedState>,
   ctes: HashSet<String, FixedState>,
   has_volatile_functions: bool,
   has_writes: bool,
   schema_change: bool,
   mutates_session: bool,
}

impl Default for AstTableExtractor {
   fn default() -> Self {
      Self {
         tables: Vec::new(),
         functions: Vec::new(),
         seen_tables: HashSet::with_hasher(FixedState::with_seed(0)),
         seen_functions: HashSet::with_hasher(FixedState::with_seed(0)),
         ctes: HashSet::with_hasher(FixedState::with_seed(0)),
         has_volatile_functions: false,
         has_writes: false,
         schema_change: false,
         mutates_session: false,
      }
   }
}

impl Visitor for AstTableExtractor {
   type Break = ();

   fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
      if is_write_statement(statement) {
         self.has_writes = true;
      }
      if is_schema_change(statement) {
         self.schema_change = true;
      }
      ControlFlow::Continue(())
   }

   fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
      if let Some(with) = &query.with {
         for cte in &with.cte_tables {
            self.ctes.insert(cte.alias.name.value.to_lowercase());
         }
      }
      ControlFlow::Continue(())
   }

   fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
      let parts: Vec<String> = relation
         .0
         .iter()
         .map(|part| match part {
            ObjectNamePart::Identifier(ident) => ident.value.to_ascii_lowercase(),
            other => other.to_string().to_ascii_lowercase(),
         })
         .collect();
      let Some(bare) = parts.last() else {
         return ControlFlow::Continue(());
      };
      if self.ctes.contains(bare) {
         return ControlFlow::Continue(());
      }
      push_relation(&mut self.tables, &mut self.seen_tables, &parts);
      ControlFlow::Continue(())
   }

   fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
      if let Expr::Function(func) = expr {
         let parts: Vec<String> = func
            .name
            .0
            .iter()
            .map(|part| match part {
               ObjectNamePart::Identifier(ident) => ident.value.to_ascii_lowercase(),
               other => other.to_string().to_ascii_lowercase(),
            })
            .collect();
         let Some(bare) = parts.last() else {
            return ControlFlow::Continue(());
         };
         if is_volatile_func(bare) {
            self.has_volatile_functions = true;
         }
         if bare == "set_config" {
            self.mutates_session = true;
         }
         let name = if parts.len() > 1 {
            parts.join(".")
         } else {
            bare.clone()
         };
         if self.seen_functions.insert(name.clone()) {
            self.functions.push(name);
         }
      }
      ControlFlow::Continue(())
   }
}

// Registers both the qualified and bare name so unqualified writes still evict
// schema-qualified cache entries.
fn push_relation(
   tables: &mut Vec<String>,
   seen: &mut HashSet<String, FixedState>,
   parts: &[String],
) {
   if parts.len() > 1 {
      let full = parts.join(".");
      if seen.insert(full.clone()) {
         tables.push(full);
      }
   }
   if let Some(bare) = parts.last()
      && seen.insert(bare.clone())
   {
      tables.push(bare.clone());
   }
}

// Checks for non-deterministic or session-dependent SQL functions whose
// results must not be shared between clients.
fn is_volatile_func(name: &str) -> bool {
   matches!(
      name,
      "now"
         | "current_timestamp"
         | "current_time"
         | "current_date"
         | "timeofday"
         | "clock_timestamp"
         | "statement_timestamp"
         | "transaction_timestamp"
         | "random"
         | "uuid_generate_v4"
         | "gen_random_uuid"
         | "pg_backend_pid"
         | "nextval"
         | "currval"
         | "lastval"
         | "setval"
         | "current_user"
         | "session_user"
         | "current_role"
         | "current_schema"
         | "current_catalog"
         | "current_setting"
         | "txid_current"
         | "inet_client_addr"
         | "pg_sleep"
   )
}

// Parses and analyzes SQL queries using the AST visitor, falling back to a
// lexical scanner for vendor syntax.
pub fn parse_sql(raw: &str) -> ParsedQuery {
   let trimmed = raw.trim();
   let normalized = normalize_sql(trimmed);
   let fingerprint = compute_fingerprint(&normalized);

   let dialect = PostgreSqlDialect {};
   let ast = Parser::parse_sql(&dialect, trimmed)
      .or_else(|_| Parser::parse_sql(&GenericDialect {}, trimmed));

   if let Ok(statements) = ast
      && !statements.is_empty()
   {
      let mut extractor = AstTableExtractor::default();
      let mut session_changes = Vec::new();
      for stmt in &statements {
         let _ = stmt.visit(&mut extractor);
         if let Some(change) = session_change_for(stmt, &normalized) {
            session_changes.push(change);
         }
         // COPY FROM names its target table directly rather than through a
         // query, so collect it explicitly.
         if let Statement::Copy {
            source: CopySource::Table { table_name, .. },
            ..
         } = stmt
         {
            let parts: Vec<String> = table_name
               .0
               .iter()
               .map(|part| match part {
                  ObjectNamePart::Identifier(ident) => ident.value.to_ascii_lowercase(),
                  other => other.to_string().to_ascii_lowercase(),
               })
               .collect();
            push_relation(&mut extractor.tables, &mut extractor.seen_tables, &parts);
         }
      }

      let kind = match &statements[0] {
         Statement::Query(_) => QueryKind::Select,
         Statement::Insert { .. } => QueryKind::Insert,
         Statement::Update { .. } => QueryKind::Update,
         Statement::Delete { .. } => QueryKind::Delete,
         Statement::StartTransaction { .. } => QueryKind::Begin,
         Statement::Commit { .. } => QueryKind::Commit,
         // ROLLBACK TO SAVEPOINT keeps the transaction open, so it must not
         // be treated as a transaction end.
         Statement::Rollback {
            savepoint: None, ..
         } => QueryKind::Rollback,
         Statement::Rollback { .. } => QueryKind::Other,
         Statement::Set { .. } => QueryKind::Set,
         Statement::CreateTable { .. }
         | Statement::Drop { .. }
         | Statement::AlterTable { .. }
         | Statement::Truncate { .. } => QueryKind::Ddl,
         _ => QueryKind::Other,
      };

      let is_cacheable = statements.len() == 1
         && kind == QueryKind::Select
         && is_plain_select(&statements[0])
         && !extractor.has_volatile_functions
         && !extractor.tables.is_empty()
         && !extractor
            .tables
            .iter()
            .any(|table| is_system_relation(table));
      let writes = extractor.has_writes;

      let catalog_independent = is_cacheable && extractor.functions.is_empty();
      return ParsedQuery {
         kind,
         tables: extractor.tables,
         normalized,
         fingerprint,
         is_cacheable,
         writes,
         functions: extractor.functions,
         session_changes,
         schema_change: extractor.schema_change,
         mutates_session: extractor.mutates_session,
         catalog_independent,
      };
   }

   fallback_parse_sql(trimmed, normalized, fingerprint)
}

// System catalogs and statistics views reflect live server state, so their
// results are never shared between sessions.
fn is_system_relation(name: &str) -> bool {
   let bare = name.rsplit('.').next().unwrap_or(name);
   bare.starts_with("pg_") || name.starts_with("information_schema.")
}

// Translates SET/RESET/DISCARD into a session-context update. The normalized
// statement text is hashed into the context rather than interpreted, so any
// spelling of the same change yields the same key on every connection.
fn session_change_for(statement: &Statement, normalized: &str) -> Option<SessionChange> {
   match statement {
      Statement::Set(set) => {
         let local = match set {
            Set::SingleAssignment { scope, .. } => {
               matches!(scope, Some(ContextModifier::Local))
            }
            Set::SetTimeZone { local, .. } => *local,
            Set::SetRole {
               context_modifier, ..
            } => matches!(context_modifier, Some(ContextModifier::Local)),
            // SET TRANSACTION applies to the current transaction only.
            Set::SetTransaction { .. } => true,
            _ => false,
         };
         Some(SessionChange::Set {
            statement: normalized.to_string(),
            local,
         })
      }
      Statement::Reset(_) => Some(SessionChange::Reset),
      Statement::Discard {
         object_type: DiscardObject::ALL,
      } => Some(SessionChange::DiscardAll),
      _ => None,
   }
}

// Locking or side-effecting SELECTs (FOR UPDATE/SHARE, data-modifying CTEs)
// must reach the origin.
fn is_plain_select(stmt: &Statement) -> bool {
   match stmt {
      Statement::Query(query) => query_is_read_only(query),
      _ => false,
   }
}

fn query_is_read_only(query: &Query) -> bool {
   query.locks.is_empty()
      && query.for_clause.is_none()
      && set_expr_is_read_only(&query.body)
      && query.with.as_ref().is_none_or(|with| {
         with
            .cte_tables
            .iter()
            .all(|cte| query_is_read_only(&cte.query))
      })
}

fn set_expr_is_read_only(body: &SetExpr) -> bool {
   match body {
      SetExpr::Select(_) | SetExpr::Values(_) | SetExpr::Table(_) => true,
      SetExpr::Query(query) => query_is_read_only(query),
      SetExpr::SetOperation { left, right, .. } => {
         set_expr_is_read_only(left) && set_expr_is_read_only(right)
      }
      SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_) | SetExpr::Merge(_) => false,
   }
}

fn is_write_statement(stmt: &Statement) -> bool {
   matches!(
      stmt,
      Statement::Insert { .. }
         | Statement::Update { .. }
         | Statement::Delete { .. }
         | Statement::Merge(_)
         | Statement::CreateTable { .. }
         | Statement::CreateView { .. }
         | Statement::Drop { .. }
         | Statement::AlterTable { .. }
         | Statement::Truncate { .. }
         // COPY TO only reads; COPY FROM table (or a query) loads rows.
         | Statement::Copy { to: false, .. }
   )
}

// Statements that can change table definitions, view definitions, or function
// volatility, invalidating the catalog snapshot used for cache decisions.
fn is_schema_change(stmt: &Statement) -> bool {
   matches!(
      stmt,
      Statement::CreateTable { .. }
         | Statement::CreateView { .. }
         | Statement::CreateFunction(_)
         | Statement::CreateProcedure { .. }
         | Statement::CreateTrigger(_)
         | Statement::CreateType { .. }
         | Statement::CreateSequence { .. }
         | Statement::CreateExtension(_)
         | Statement::AlterTable { .. }
         | Statement::AlterView { .. }
         | Statement::AlterFunction(_)
         | Statement::Drop { .. }
   )
}

// Fallback scanner for non-standard statements or custom vendor syntax.
fn fallback_parse_sql(trimmed: &str, normalized: String, fingerprint: u64) -> ParsedQuery {
   let lower = trimmed.to_ascii_lowercase();

   let kind = if lower.starts_with("select") || lower.starts_with("with") {
      QueryKind::Select
   } else if lower.starts_with("insert") {
      QueryKind::Insert
   } else if lower.starts_with("update") {
      QueryKind::Update
   } else if lower.starts_with("delete") {
      QueryKind::Delete
   } else if lower.starts_with("begin") || lower.starts_with("start transaction") {
      QueryKind::Begin
   } else if lower.starts_with("commit") {
      QueryKind::Commit
   } else if lower.starts_with("rollback") {
      QueryKind::Rollback
   } else if lower.starts_with("set ") {
      QueryKind::Set
   } else {
      QueryKind::Other
   };

   let tables = extract_tables_fallback(&lower, kind);
   let single_statement = count_statements(trimmed) == 1;
   let data_modifying = (kind == QueryKind::Select
      && (lower.contains(" insert ") || lower.contains(" update ") || lower.contains(" delete ")))
      || lower.contains("for update")
      || lower.contains("for share");
   let is_cacheable = kind == QueryKind::Select
      && !tables.is_empty()
      && !tables.iter().any(|table| is_system_relation(table))
      && single_statement
      && !data_modifying
      && !has_volatile_keyword(&lower);
   let writes = matches!(
      kind,
      QueryKind::Insert | QueryKind::Update | QueryKind::Delete | QueryKind::Ddl
   ) || has_write_keyword(trimmed);
   let session_changes = fallback_session_changes(&normalized);

   ParsedQuery {
      kind,
      tables,
      normalized,
      fingerprint,
      is_cacheable,
      writes,
      functions: Vec::new(),
      session_changes,
      schema_change: lower.starts_with("create ")
         || lower.starts_with("alter ")
         || lower.starts_with("drop "),
      mutates_session: lower.contains("set_config("),
      // Fallback path never extracts functions, so always catalog_independent.
      catalog_independent: is_cacheable,
   }
}

fn fallback_session_changes(normalized: &str) -> Vec<SessionChange> {
   let mut changes = Vec::new();
   for stmt in split_top_level(normalized) {
      let stmt = stmt.trim();
      if stmt.starts_with("set ") {
         changes.push(SessionChange::Set {
            statement: normalized.to_string(),
            local: stmt.starts_with("set local "),
         });
      } else if stmt.starts_with("reset ") {
         changes.push(SessionChange::Reset);
      } else if stmt == "discard all" {
         changes.push(SessionChange::DiscardAll);
      }
   }
   changes
}

// Splits on semicolons outside quoted literals and identifiers.
fn split_top_level(sql: &str) -> Vec<String> {
   let mut parts = Vec::new();
   let mut current = String::new();
   let mut in_quote = false;
   let mut quote_char = ' ';
   for ch in sql.chars() {
      if in_quote {
         current.push(ch);
         if ch == quote_char {
            in_quote = false;
         }
         continue;
      }
      if ch == '\'' || ch == '"' {
         in_quote = true;
         quote_char = ch;
         current.push(ch);
      } else if ch == ';' {
         if !current.trim().is_empty() {
            parts.push(std::mem::take(&mut current));
         }
      } else {
         current.push(ch);
      }
   }
   if !current.trim().is_empty() {
      parts.push(current);
   }
   parts
}

// Scans keywords outside string/identifier literals so a SELECT followed by a
// write still invalidates. False positives only cost cache misses.
fn has_write_keyword(sql: &str) -> bool {
   const WRITE_KEYWORDS: [&str; 9] = [
      "insert", "update", "delete", "truncate", "create", "drop", "alter", "merge", "copy",
   ];
   let mut in_quote = false;
   let mut quote_char = b' ';
   let mut word = [0u8; 12];
   let mut wlen = 0usize;

   for &b in sql.as_bytes() {
      if in_quote {
         if b == quote_char {
            in_quote = false;
         }
         continue;
      }
      if b == b'\'' || b == b'"' {
         in_quote = true;
         quote_char = b;
         wlen = 0;
         continue;
      }
      if b.is_ascii_alphanumeric() || b == b'_' {
         if wlen < 12 {
            word[wlen] = b.to_ascii_lowercase();
            wlen += 1;
         }
         continue;
      }
      if wlen > 0 {
         if WRITE_KEYWORDS.contains(&unsafe { std::str::from_utf8_unchecked(&word[..wlen]) }) {
            return true;
         }
         wlen = 0;
      }
   }
   wlen > 0 && WRITE_KEYWORDS.contains(&unsafe { std::str::from_utf8_unchecked(&word[..wlen]) })
}

fn has_volatile_keyword(lower: &str) -> bool {
   [
      "now(",
      "random(",
      "nextval(",
      "currval(",
      "current_timestamp",
      "current_user",
      "pg_backend_pid(",
   ]
   .iter()
   .any(|needle| lower.contains(needle))
}

// Counts top-level statement terminators outside string/identifier literals.
fn count_statements(sql: &str) -> usize {
   let mut count = 1;
   let mut in_quote = false;
   let mut quote_char = 0u8;
   for &b in sql.as_bytes() {
      if in_quote {
         if b == quote_char {
            in_quote = false;
         }
         continue;
      }
      if b == b'\'' || b == b'"' {
         in_quote = true;
         quote_char = b;
      } else if b == b';' {
         count += 1;
      }
   }
   if sql.trim_end().ends_with(';') {
      count -= 1;
   }
   count.max(1)
}

// Normalizes SQL whitespace and lowercases keywords while preserving literal values.
pub fn normalize_sql(sql: &str) -> String {
   let bytes = sql.as_bytes();
   let mut out = Vec::with_capacity(bytes.len());
   let mut in_str = false;
   let mut quote_char = 0u8;
   let mut in_whitespace = false;

   for &b in bytes {
      if in_str {
         out.push(b);
         if b == quote_char {
            in_str = false;
         }
         continue;
      }
      if b == b'\'' || b == b'"' {
         in_str = true;
         quote_char = b;
         out.push(b);
         continue;
      }
      if b == b' ' || b == b'\t' || b == b'\n' || b == b'\r' {
         if !in_whitespace {
            out.push(b' ');
            in_whitespace = true;
         }
      } else {
         in_whitespace = false;
         out.push(b.to_ascii_lowercase());
      }
   }
   // SAFETY: ASCII bytes are lowercased (still ASCII); multi-byte UTF-8 sequences
   // (bytes ≥ 0x80) pass through unchanged, so output is valid UTF-8.
   let s = unsafe { String::from_utf8_unchecked(out) };
   s.trim().to_string()
}

fn extract_tables_fallback(lower: &str, kind: QueryKind) -> Vec<String> {
   let mut tables = Vec::new();
   let mut seen: HashSet<String, FixedState> = HashSet::with_hasher(FixedState::with_seed(0));
   let tokens: Vec<&str> = lower.split_whitespace().collect();

   for i in 0..tokens.len() {
      let tok = tokens[i];
      let target = match kind {
         QueryKind::Select => tok == "from" || tok == "join",
         QueryKind::Insert => tok == "into",
         QueryKind::Update => tok == "update",
         QueryKind::Delete => tok == "from",
         _ => false,
      };

      if target && i + 1 < tokens.len() {
         let candidate = clean_table_name(tokens[i + 1]);
         if !candidate.is_empty() && seen.insert(candidate.clone()) {
            tables.push(candidate);
         }
      }
   }

   tables
}

fn clean_table_name(name: &str) -> String {
   name
      .trim_matches(|c: char| c == '"' || c == '\'' || c == '`' || c == ';' || c == '(' || c == ')')
      .split('.')
      .next_back()
      .unwrap_or("")
      .to_string()
}

pub fn compute_fingerprint(sql: &str) -> u64 {
   let mut hasher = FixedState::with_seed(0).build_hasher();
   hasher.write(sql.as_bytes());
   hasher.finish()
}

// Fingerprint including bound parameter values for prepared statements.
pub fn compute_fingerprint_with_params(
   normalized_sql: &str,
   params: &[Option<Vec<u8>>],
   param_formats: &[i16],
) -> u64 {
   let base = compute_fingerprint(normalized_sql);
   compute_fingerprint_with_params_base(base, params, param_formats)
}

// Computes parameter-bound fingerprint directly from a pre-calculated SQL base fingerprint.
pub fn compute_fingerprint_with_params_base(
   base_fingerprint: u64,
   params: &[Option<Vec<u8>>],
   param_formats: &[i16],
) -> u64 {
   let mut hasher = FixedState::with_seed(0).build_hasher();
   hasher.write_u64(base_fingerprint);
   hasher.write_u8(0xFF);
   hasher.write_u32(params.len() as u32);
   for (index, param) in params.iter().enumerate() {
      let format = param_formats.get(index).copied().unwrap_or(0);
      hasher.write_i16(format);
      match param {
         None => hasher.write_i32(-1),
         Some(bytes) => {
            hasher.write_i32(bytes.len() as i32);
            hasher.write(bytes);
         }
      }
   }
   hasher.finish()
}

const HEX: &[u8; 16] = b"0123456789abcdef";

// Builds the cache key for this query result in the shared key-value store.
pub fn make_cache_key(prefix: &str, table: &str, fingerprint: u64) -> String {
   let mut key = String::with_capacity(prefix.len() + table.len() + 17);
   key.push_str(prefix);
   key.push_str(table);
   key.push(':');
   let b = fingerprint.to_be_bytes();
   let hex = [
      HEX[(b[0] >> 4) as usize],
      HEX[(b[0] & 0xF) as usize],
      HEX[(b[1] >> 4) as usize],
      HEX[(b[1] & 0xF) as usize],
      HEX[(b[2] >> 4) as usize],
      HEX[(b[2] & 0xF) as usize],
      HEX[(b[3] >> 4) as usize],
      HEX[(b[3] & 0xF) as usize],
      HEX[(b[4] >> 4) as usize],
      HEX[(b[4] & 0xF) as usize],
      HEX[(b[5] >> 4) as usize],
      HEX[(b[5] & 0xF) as usize],
      HEX[(b[6] >> 4) as usize],
      HEX[(b[6] & 0xF) as usize],
      HEX[(b[7] >> 4) as usize],
      HEX[(b[7] & 0xF) as usize],
   ];
   // SAFETY: all bytes are ASCII hex digits.
   key.push_str(unsafe { std::str::from_utf8_unchecked(&hex) });
   key
}

// Builds the cache key directly into `buf` without a heap allocation.
// Returns a `&str` into the buffer. Falls back to an empty str if the key
// would exceed the buffer — callers that get "" must use `make_cache_key`.
pub fn make_cache_key_inline<'a>(
   prefix: &str,
   table: &str,
   fingerprint: u64,
   buf: &'a mut [u8; 512],
) -> &'a str {
   let p = prefix.as_bytes();
   let t = table.as_bytes();
   let needed = p.len() + t.len() + 17; // 1 ':' + 16 hex chars
   if needed > buf.len() {
      return "";
   }
   let mut pos = 0;
   buf[pos..pos + p.len()].copy_from_slice(p);
   pos += p.len();
   buf[pos..pos + t.len()].copy_from_slice(t);
   pos += t.len();
   buf[pos] = b':';
   pos += 1;
   let b = fingerprint.to_be_bytes();
   for byte in b {
      buf[pos] = HEX[(byte >> 4) as usize];
      buf[pos + 1] = HEX[(byte & 0xF) as usize];
      pos += 2;
   }
   // SAFETY: all bytes are ASCII.
   unsafe { std::str::from_utf8_unchecked(&buf[..pos]) }
}

// Sharded SQL analysis cache: parsing is the dominant cost on the hot path,
// so each distinct statement text is parsed only once per process.
pub struct ParsedQueryCache {
   shards: Box<[RwLock<HashMap<u64, ParsedEntry, FixedState>>]>,
   max_per_shard: usize,
}

struct ParsedEntry {
   parsed: Arc<ParsedQuery>,
}

impl Default for ParsedQueryCache {
   fn default() -> Self {
      Self::new(64, 2048)
   }
}

impl ParsedQueryCache {
   pub fn new(shard_count: usize, max_per_shard: usize) -> Self {
      let shard_count = shard_count.next_power_of_two().max(1);
      let mut shards = Vec::with_capacity(shard_count);
      for _ in 0..shard_count {
         shards.push(RwLock::new(HashMap::with_hasher(FixedState::with_seed(0))));
      }
      Self {
         shards: shards.into_boxed_slice(),
         max_per_shard,
      }
   }

   pub fn get_or_parse(&self, sql: &str) -> Arc<ParsedQuery> {
      let hash = hash_sql(sql);
      let index = (hash as usize) & (self.shards.len() - 1);
      let shard = &self.shards[index];

      if let Some(entry) = shard.read().get(&hash) {
         return Arc::clone(&entry.parsed);
      }

      let parsed = Arc::new(parse_sql(sql));
      let mut guard = shard.write();
      if guard.len() >= self.max_per_shard
         && !guard.contains_key(&hash)
         && let Some(evict) = guard.keys().next().copied()
      {
         guard.remove(&evict);
      }
      guard.insert(
         hash,
         ParsedEntry {
            parsed: Arc::clone(&parsed),
         },
      );
      parsed
   }

   pub fn len(&self) -> usize {
      self.shards.iter().map(|shard| shard.read().len()).sum()
   }

   pub fn is_empty(&self) -> bool {
      self.len() == 0
   }
}

fn hash_sql(sql: &str) -> u64 {
   let mut hasher = FixedState::with_seed(0x5bd1_e995).build_hasher();
   hasher.write(sql.as_bytes());
   hasher.finish()
}
