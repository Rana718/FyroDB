use super::doc::{collect_keys, count_docs};
use crate::search::{FieldDef, FieldType, IndexDef, StorageKind, parse_field_defs};
use crate::storage::store::Store;
use crate::utils::resp;
use std::sync::Arc;

pub(super) fn ft_create(parts: &[&str], store: &Arc<Store>, out: &mut Vec<u8>) {
   if parts.len() < 3 {
      return resp::write_store_err(out, "ERR wrong number of arguments for 'ft.create'");
   }
   let name = parts[1];
   let mut prefixes: Vec<String> = Vec::new();
   let mut storage = StorageKind::Hash;
   let mut i = 2;
   while i < parts.len() {
      match parts[i].to_ascii_uppercase().as_str() {
         "ON" => {
            if parts
               .get(i + 1)
               .is_some_and(|s| s.eq_ignore_ascii_case("JSON"))
            {
               storage = StorageKind::Json;
            }
            i += 2;
         }
         "PREFIX" => {
            let Some(count) = parts.get(i + 1).and_then(|c| c.parse::<usize>().ok()) else {
               return resp::write_store_err(out, "ERR bad PREFIX count");
            };
            i += 2;
            for _ in 0..count {
               match parts.get(i) {
                  Some(p) => {
                     prefixes.push(p.to_string());
                     i += 1;
                  }
                  None => return resp::write_store_err(out, "ERR bad PREFIX list"),
               }
            }
         }
         "STOPWORDS" => {
            let Some(count) = parts.get(i + 1).and_then(|c| c.parse::<usize>().ok()) else {
               return resp::write_store_err(out, "ERR bad STOPWORDS count");
            };
            i += 2 + count;
         }
         "TEMPORARY" => i += 2,
         "SCHEMA" => {
            i += 1;
            break;
         }
         _ => i += 1,
      }
   }

   let schema = &parts[i.min(parts.len())..];
   let fields = match parse_field_defs(schema) {
      Ok(f) if !f.is_empty() => f,
      Ok(_) => return resp::write_store_err(out, "ERR schema needs at least one field"),
      Err((e, _)) => return resp::write_err(out, e),
   };
   let def = IndexDef {
      name: name.to_string(),
      prefixes,
      storage,
      fields,
      created_at_ms: crate::storage::value::now_ms(),
      num_docs: std::sync::atomic::AtomicU64::new(0),
   };
   match store.search_state().create(def) {
      Ok(()) => {
         // Kick background HNSW graph builds now so the first KNN query
         // does not stall on graph construction.
         if let Some(def) = store.search_state().def_arc(name) {
            store.search_state().prefetch_build(store, &def);
         }
         resp::write_ok(out)
      }
      Err(e) => resp::write_err(out, e),
   }
}

pub(super) fn ft_info(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let Some(name) = parts.get(1) else {
      return resp::write_store_err(out, "ERR wrong number of arguments for 'ft.info'");
   };
   let Some(def) = store.search_state().def_arc(name) else {
      return resp::write_store_err(out, "ERR Unknown index name");
   };

   let num_docs = store
      .search_state()
      .cached_doc_count(&def.name, store.dbsize(), || count_docs(store, &def));
   let doc_str = num_docs.to_string();
   let num_records = num_docs * def.fields.len() as u64;
   let rec_str = num_records.to_string();
   let doc_mb = format!("{:.3}", num_docs as f64 * 104.0 / (1024.0 * 1024.0));
   let (indexing, percent) = store.search_state().index_progress(&def, num_docs);
   let vec_bytes: usize = def
      .fields
      .iter()
      .filter(|f| f.vector.is_some())
      .filter_map(|f| store.search_state().hnsw_entry(&def.name, &f.name))
      .map(|e| e.graph.memory_bytes())
      .sum();

   resp::write_array_header(out, 26);

   resp::write_bulk(out, "index_name");
   resp::write_bulk(out, &def.name);
   resp::write_bulk(out, "index_options");
   resp::write_array_header(out, 0);
   resp::write_bulk(out, "index_definition");
   resp::write_array_header(out, 6);
   resp::write_bulk(out, "key_type");
   resp::write_bulk(out, def.storage.as_str());
   resp::write_bulk(out, "prefixes");
   resp::write_array_header(out, def.prefixes.len());
   for p in &def.prefixes {
      resp::write_bulk(out, p);
   }
   resp::write_bulk(out, "default_score");
   resp::write_bulk(out, "1");
   resp::write_bulk(out, "attributes");
   resp::write_array_header(out, def.fields.len());
   for f in &def.fields {
      write_field_info(out, f);
   }
   resp::write_bulk(out, "num_docs");
   resp::write_bulk(out, &doc_str);
   resp::write_bulk(out, "num_records");
   resp::write_bulk(out, &rec_str);
   resp::write_bulk(out, "inverted_sz_mb");
   resp::write_bulk(out, "0");
   resp::write_bulk(out, "total_indexing_time");
   resp::write_bulk(out, "0");
   resp::write_bulk(out, "offset_vectors_sz_mb");
   resp::write_bulk(out, "0");
   resp::write_bulk(out, "doc_table_size_mb");
   resp::write_bulk(out, &doc_mb);
   resp::write_bulk(out, "indexing");
   resp::write_bulk(out, if indexing { "1" } else { "0" });
   resp::write_bulk(out, "percent_indexed");
   resp::write_bulk(out, &format!("{percent:.4}"));
   resp::write_bulk(out, "vector_index_sz_mb");
   resp::write_bulk(out, &format!("{:.3}", vec_bytes as f64 / (1024.0 * 1024.0)));
}

pub(super) fn write_field_info(out: &mut Vec<u8>, f: &FieldDef) {
   if f.field_type == FieldType::Vector
      && let Some(v) = &f.vector
   {
      resp::write_array_header(out, 22);
      resp::write_bulk(out, "identifier");
      resp::write_bulk(out, &f.name);
      resp::write_bulk(out, "attribute");
      resp::write_bulk(out, f.alias.as_deref().unwrap_or(&f.name));
      resp::write_bulk(out, "type");
      resp::write_bulk(out, "VECTOR");
      resp::write_bulk(out, "algorithm");
      resp::write_bulk(out, v.algorithm);
      resp::write_bulk(out, "data_type");
      resp::write_bulk(out, v.dtype);
      resp::write_bulk(out, "dim");
      resp::write_bulk(out, &v.dims.to_string());
      resp::write_bulk(out, "distance_metric");
      resp::write_bulk(out, v.metric.as_str());
      resp::write_bulk(out, "M");
      resp::write_bulk(out, &v.m.to_string());
      resp::write_bulk(out, "ef_construction");
      resp::write_bulk(out, &v.ef_construction.to_string());
      resp::write_bulk(out, "quantization");
      resp::write_bulk(out, v.quant.as_str());
      resp::write_bulk(out, "flags");
      resp::write_array_header(out, 0);
      return;
   }
   resp::write_array_header(out, 8 + usize::from(f.sortable));
   resp::write_bulk(out, "identifier");
   resp::write_bulk(out, &f.name);
   resp::write_bulk(out, "attribute");
   resp::write_bulk(out, f.alias.as_deref().unwrap_or(&f.name));
   resp::write_bulk(out, "type");
   resp::write_bulk(out, f.field_type.as_str());
   resp::write_bulk(out, "flags");
   resp::write_array_header(out, if f.sortable { 1 } else { 0 });
   if f.sortable {
      resp::write_bulk(out, "SORTABLE");
   }
}

pub(super) fn ft_dropindex(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let Some(name) = parts.get(1) else {
      return resp::write_store_err(out, "ERR wrong number of arguments for 'ft.dropindex'");
   };
   let dd = parts.get(2).is_some_and(|s| s.eq_ignore_ascii_case("DD"));
   let Some(def) = store.search_state().def_arc(name) else {
      return resp::write_store_err(out, "ERR Unknown index name");
   };
   if dd {
      let keys = collect_keys(store, &def);
      for key in keys {
         store.del(&key);
      }
   }
   match store.search_state().drop_index(name) {
      Ok(()) => {
         rust_zmalloc::purge();
         resp::write_ok(out)
      }
      Err(e) => resp::write_err(out, e),
   }
}

pub(super) fn ft_alter(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let Some(name) = parts.get(1) else {
      return resp::write_store_err(out, "ERR wrong number of arguments for 'ft.alter'");
   };
   let Some(add_pos) = parts.iter().position(|p| p.eq_ignore_ascii_case("ADD")) else {
      return resp::write_store_err(out, "ERR ADD required");
   };
   let schema = &parts[add_pos + 1..];
   match parse_field_defs(schema) {
      Ok(fields) => match store.search_state().alter(name, fields) {
         Ok(()) => resp::write_ok(out),
         Err(e) => resp::write_err(out, e),
      },
      Err((e, _)) => resp::write_err(out, e),
   }
}

pub(super) enum AliasOp {
   Add,
   Update,
   Del,
}

pub(super) fn ft_alias(parts: &[&str], store: &Store, out: &mut Vec<u8>, op: AliasOp) {
   let Some(alias) = parts.get(1) else {
      return resp::write_store_err(out, "ERR wrong number of arguments");
   };
   let result = match op {
      AliasOp::Del => store.search_state().alias_del(alias),
      AliasOp::Add | AliasOp::Update => {
         let Some(target) = parts.get(2) else {
            return resp::write_store_err(out, "ERR wrong number of arguments");
         };
         store
            .search_state()
            .alias_add(alias, target, matches!(op, AliasOp::Update))
      }
   };
   match result {
      Ok(()) => resp::write_ok(out),
      Err(e) => resp::write_err(out, e),
   }
}

pub(super) fn ft_aliaslist(store: &Store, out: &mut Vec<u8>) {
   let aliases = store.search_state().alias_list();
   resp::write_array_header(out, aliases.len());
   for a in aliases {
      resp::write_bulk(out, &a);
   }
}

pub(super) fn ft_config(parts: &[&str], out: &mut Vec<u8>) {
   let Some(action) = parts.get(1) else {
      return resp::write_store_err(out, "ERR wrong number of arguments");
   };
   if action.eq_ignore_ascii_case("GET") {
      return resp::write_array_header(out, 0);
   }
   if action.eq_ignore_ascii_case("SET") || action.eq_ignore_ascii_case("HELP") {
      return resp::write_ok(out);
   }
   resp::write_store_err(out, "ERR unknown config action")
}
