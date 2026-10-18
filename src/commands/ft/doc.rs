use crate::search::query::Cond;
use crate::search::{IndexDef, StorageKind};
use crate::storage::store::Store;
use crate::storage::value::{FyroDB, StoreValue};

pub(super) struct SearchOpts {
   pub(super) no_content: bool,
   pub(super) with_scores: bool,
   pub(super) with_payloads: bool,
   pub(super) limit_offset: usize,
   pub(super) limit_num: usize,
   pub(super) return_fields: Vec<String>,
   pub(super) return_all: bool,
   pub(super) sortby_field: Option<String>,
   pub(super) sortby_desc: bool,
   pub(super) params: Vec<(String, Vec<u8>)>,
   pub(super) ids: Vec<String>,
}

impl Default for SearchOpts {
   fn default() -> Self {
      Self {
         no_content: false,
         with_scores: false,
         with_payloads: false,
         limit_offset: 0,
         limit_num: 10,
         return_fields: Vec::new(),
         return_all: true,
         sortby_field: None,
         sortby_desc: false,
         params: Vec::new(),
         ids: Vec::new(),
      }
   }
}
pub(super) struct DocHit {
   pub(super) key: String,
   pub(super) score: f64,
}

pub(super) fn count_docs(store: &Store, def: &IndexDef) -> u64 {
   let mut n = 0u64;
   store.data.for_each(|key, val| {
      if !val.is_expired() && def.matches_key(key) && doc_storage_matches(val, def) {
         n += 1;
      }
   });
   n
}

pub(super) fn collect_keys(store: &Store, def: &IndexDef) -> Vec<String> {
   let mut keys = Vec::new();
   store.data.for_each(|key, val| {
      if !val.is_expired() && def.matches_key(key) && doc_storage_matches(val, def) {
         keys.push(key.to_string());
      }
   });
   keys
}

pub(super) fn doc_storage_matches(val: &StoreValue, def: &IndexDef) -> bool {
   match def.storage {
      StorageKind::Hash => matches!(val.value, FyroDB::Hash(_)),
      StorageKind::Json => matches!(val.value, FyroDB::Json(_)),
   }
}
pub(super) fn doc_vector<'a>(val: &'a StoreValue, def: &IndexDef, field: &str) -> Option<&'a [u8]> {
   match (&val.value, def.storage) {
      (FyroDB::Hash(h), StorageKind::Hash) => h.get(field).map(|v| v.as_bytes()),
      _ => None,
   }
}

pub(super) fn doc_fields(
   val: &StoreValue,
   def: &IndexDef,
   opts: &SearchOpts,
) -> Vec<(String, Vec<u8>)> {
   let mut out_fields: Vec<(String, Vec<u8>)> = Vec::new();
   match (&val.value, def.storage) {
      (FyroDB::Hash(h), StorageKind::Hash) => {
         if opts.return_all {
            for (k, v) in h.iter() {
               out_fields.push((k.to_string(), v.as_bytes().to_vec()));
            }
         } else {
            for f in &opts.return_fields {
               if let Some(v) = h.get(f) {
                  out_fields.push((f.clone(), v.as_bytes().to_vec()));
               }
            }
         }
      }
      (FyroDB::Json(s), StorageKind::Json) => {
         let text = &**s;
         let Some(json) = crate::storage::value::JsonValue::parse(text) else {
            return out_fields;
         };
         if opts.return_all {
            out_fields.push(("$".to_string(), json.to_resp_string().into_bytes()));
         } else {
            for f in &opts.return_fields {
               let path = if f.starts_with("$.") {
                  f.clone()
               } else {
                  format!("$.{f}")
               };
               if let Some(v) = json.get_path(&path) {
                  out_fields.push((f.clone(), json_value_bytes(v)));
               }
            }
         }
      }
      _ => {}
   }
   out_fields
}

pub(super) fn json_value_bytes(v: &crate::storage::value::JsonValue) -> Vec<u8> {
   match v {
      crate::storage::value::JsonValue::String(s) => s.as_bytes().to_vec(),
      other => other.to_resp_string().into_bytes(),
   }
}

pub(super) fn cond_matches(cond: &Cond, val: &StoreValue, def: &IndexDef) -> bool {
   match cond {
      Cond::MatchAll => true,
      Cond::MatchNone => false,
      Cond::Numeric { field, range } => match doc_scalar(val, def, field) {
         Some(v) => range.matches(v),
         None => false,
      },
      Cond::Tag { field, any } => tag_cond(val, def, field, any, false),
      Cond::TagNeg { field, any } => tag_cond(val, def, field, any, true),
      Cond::Text { field, terms } => text_cond(val, def, field.as_deref(), terms, false),
      Cond::TextNeg { field, terms } => text_cond(val, def, field.as_deref(), terms, true),
      Cond::And(a, b) => cond_matches(a, val, def) && cond_matches(b, val, def),
      Cond::Or(a, b) => cond_matches(a, val, def) || cond_matches(b, val, def),
   }
}

/// Tag predicate without per-document allocations: splits stored values in place.
/// Used by filtered KNN and filter scans.
pub(super) fn tag_cond(
   val: &StoreValue,
   def: &IndexDef,
   field: &str,
   any: &[String],
   negated: bool,
) -> bool {
   let sep = def.field(field).map(|f| f.separator).unwrap_or(',');
   let hit = match (&val.value, def.storage) {
      (FyroDB::Hash(h), StorageKind::Hash) => h.get(field).is_some_and(|raw| {
         raw.as_str()
            .split(sep)
            .any(|t| any.iter().any(|a| tag_match(a, t)))
      }),
      (FyroDB::Json(s), StorageKind::Json) => {
         let Some(json) = crate::storage::value::JsonValue::parse(s) else {
            return !negated;
         };
         match json.get_path(&format!("$.{field}")) {
            Some(crate::storage::value::JsonValue::String(sv)) => sv
               .as_str()
               .split(sep)
               .any(|t| any.iter().any(|a| tag_match(a, t))),
            _ => false,
         }
      }
      _ => false,
   };
   if negated { !hit } else { hit }
}

pub(super) fn text_cond(
   val: &StoreValue,
   def: &IndexDef,
   field: Option<&str>,
   terms: &[String],
   negated: bool,
) -> bool {
   let hit = match (&val.value, def.storage) {
      (FyroDB::Hash(h), StorageKind::Hash) => match field {
         Some(f) => match h.get(f) {
            Some(text) => terms.iter().all(|t| text_match(t, text.as_str())),
            None => false,
         },
         None => terms.iter().all(|t| {
            def.fields
               .iter()
               .filter(|f| f.field_type == crate::search::FieldType::Text && !f.no_index)
               .any(|f| {
                  h.get(&f.name)
                     .is_some_and(|text| text_match(t, text.as_str()))
               })
         }),
      },
      (FyroDB::Json(s), StorageKind::Json) => {
         let text = &**s;
         terms.iter().all(|t| text_match(t, text))
      }
      _ => false,
   };
   if negated { !hit } else { hit }
}

pub(super) fn tag_match(pattern: &str, tag: &str) -> bool {
   if pattern.contains('*') {
      crate::utils::util::glob_match(pattern, tag)
   } else {
      pattern == tag
   }
}
pub(super) fn text_match(term: &str, text: &str) -> bool {
   if term.contains('*') {
      let stem = term.trim_end_matches('*').to_lowercase();
      crate::utils::util::glob_match(term, text) || crate::search::tokens(text).any(|w| w == stem)
   } else {
      match crate::search::query_term(term) {
         Some(t) => crate::search::tokens(text).any(|w| w == t),
         None => true,
      }
   }
}

pub(super) fn doc_scalar(val: &StoreValue, def: &IndexDef, field: &str) -> Option<f64> {
   match (&val.value, def.storage) {
      (FyroDB::Hash(h), _) => h
         .get(field)
         .and_then(|v| std::str::from_utf8(v.as_bytes()).ok())
         .and_then(|s| s.parse().ok()),
      (FyroDB::Json(s), _) => {
         let json = crate::storage::value::JsonValue::parse(s)?;
         let v = json.get_path(&format!("$.{field}"))?;
         match v {
            crate::storage::value::JsonValue::Number(n) => Some(*n),
            crate::storage::value::JsonValue::String(s) => s.as_str().parse().ok(),
            _ => None,
         }
      }
      _ => None,
   }
}
