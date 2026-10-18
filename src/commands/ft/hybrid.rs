use super::doc::{cond_matches, doc_fields, doc_storage_matches};
use super::search::parse_search_options;
use crate::search::Resolved;
use crate::search::query::parse_query;
use crate::storage::store::Store;
use crate::utils::resp;
use std::sync::Arc;

enum Combine {
   Rrf { constant: f64 },
   Linear { alpha: f64, beta: f64 },
}

pub(super) fn ft_hybrid(
   parts: &[&str],
   binaries: &[(usize, Vec<u8>)],
   store: &Arc<Store>,
   out: &mut Vec<u8>,
) {
   let [_, index, search_kw, query, vsim_kw, field, param, ..] = parts else {
      return resp::write_store_err(out, "ERR wrong number of arguments for 'ft.hybrid'");
   };
   if !search_kw.eq_ignore_ascii_case("SEARCH") || !vsim_kw.eq_ignore_ascii_case("VSIM") {
      return resp::write_store_err(
         out,
         "ERR syntax: FT.HYBRID idx SEARCH <query> VSIM @field $vector ...",
      );
   }
   let Some(def) = store.search_state().def_arc(index) else {
      return resp::write_store_err(out, "ERR Unknown index name");
   };
   let field = field.trim_start_matches('@');
   let Some(vattrs) = def.field(field).and_then(|f| f.vector.clone()) else {
      return resp::write_store_err(out, "ERR VSIM field is not a vector field");
   };

   let rest = &parts[7..];
   let mut k = 10usize;
   let mut ef = None;
   let mut window = 20usize;
   let mut combine = Combine::Rrf { constant: 60.0 };
   let mut i = 0;
   let num = |s: Option<&&str>| s.and_then(|v| v.parse::<f64>().ok());
   while i < rest.len() {
      match rest[i].to_ascii_uppercase().as_str() {
         "KNN" => {
            k = num(rest.get(i + 1)).map_or(k, |v| v as usize).max(1);
            i += 2;
         }
         "EF_RUNTIME" => {
            ef = num(rest.get(i + 1)).map(|v| v as usize);
            i += 2;
         }
         "WINDOW" => {
            window = num(rest.get(i + 1))
               .map_or(window, |v| v as usize)
               .clamp(1, 10_000);
            i += 2;
         }
         "COMBINE" => {
            let kind = rest.get(i + 1).map(|s| s.to_ascii_uppercase());
            i += 2;
            match kind.as_deref() {
               Some("RRF") => {
                  let mut constant = 60.0;
                  if rest
                     .get(i)
                     .is_some_and(|s| s.eq_ignore_ascii_case("CONSTANT"))
                  {
                     constant = num(rest.get(i + 1)).unwrap_or(60.0);
                     i += 2;
                  }
                  combine = Combine::Rrf { constant };
               }
               Some("LINEAR") => {
                  let (mut alpha, mut beta) = (0.5, 0.5);
                  while let Some(kw) = rest.get(i) {
                     match kw.to_ascii_uppercase().as_str() {
                        "ALPHA" => alpha = num(rest.get(i + 1)).unwrap_or(alpha),
                        "BETA" => beta = num(rest.get(i + 1)).unwrap_or(beta),
                        _ => break,
                     }
                     i += 2;
                  }
                  combine = Combine::Linear { alpha, beta };
               }
               _ => return resp::write_store_err(out, "ERR COMBINE expects RRF or LINEAR"),
            }
         }
         _ => break,
      }
   }
   let window = window.max(k);
   let shift = 7 + i - 3;
   let shifted: Vec<(usize, Vec<u8>)> = binaries
      .iter()
      .filter(|(p, _)| *p >= 7 + i)
      .map(|(p, b)| (p - shift, b.clone()))
      .collect();
   let opts = match parse_search_options(&rest[i..], &shifted) {
      Ok(o) => o,
      Err(e) => return resp::write_store_err(out, &format!("ERR {e}")),
   };
   let needle = param.trim_start_matches('$');
   let Some(qv) = opts
      .params
      .iter()
      .find(|(n, _)| n.trim_start_matches('$') == needle)
      .and_then(|(_, v)| vattrs.decode(v))
   else {
      return resp::write_store_err(out, "ERR query vector missing or of the wrong size");
   };
   let Ok(pq) = parse_query(&def, query) else {
      return resp::write_store_err(out, "ERR syntax error");
   };

   let text_hits = parking_lot::Mutex::new(Vec::new());
   let vec_hits = parking_lot::Mutex::new(None);
   fyro_vector::pool::global().parallel(2, |leg| {
      if leg == 0 {
         *text_hits.lock() = lexical(store, &def, pq.filter.as_ref(), window);
      } else {
         *vec_hits.lock() = vector(
            store,
            &def,
            field,
            &vattrs,
            &qv,
            window,
            ef.unwrap_or(window.max(10)),
         );
      }
   });
   let text_hits = text_hits.into_inner();
   let Some(vec_hits) = vec_hits.into_inner() else {
      return resp::write_store_err(out, "ERR vector index is still building");
   };

   let mut fused: std::collections::HashMap<String, f64, foldhash::fast::RandomState> =
      Default::default();
   match combine {
      Combine::Rrf { constant } => {
         for list in [&text_hits, &vec_hits] {
            for (rank, (key, _)) in list.iter().enumerate() {
               *fused.entry(key.clone()).or_default() += 1.0 / (constant + rank as f64 + 1.0);
            }
         }
      }
      Combine::Linear { alpha, beta } => {
         let norm = |list: &[(String, f64)]| -> Vec<(String, f64)> {
            let (lo, hi) = list
               .iter()
               .fold((f64::INFINITY, f64::NEG_INFINITY), |(l, h), x| {
                  (l.min(x.1), h.max(x.1))
               });
            let span = (hi - lo).max(1e-12);
            list
               .iter()
               .map(|(k, s)| {
                  (
                     k.clone(),
                     if list.len() == 1 {
                        1.0
                     } else {
                        (s - lo) / span
                     },
                  )
               })
               .collect()
         };
         for (key, s) in norm(&text_hits) {
            *fused.entry(key).or_default() += alpha * s;
         }
         for (key, s) in norm(&vec_hits) {
            *fused.entry(key).or_default() += beta * s;
         }
      }
   }
   let mut ranked: Vec<(String, f64)> = fused.into_iter().collect();
   ranked.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
   let total = ranked.len();
   let s = opts.limit_offset.min(total);
   let e = (s + opts.limit_num).min(total);
   let page = &ranked[s..e];

   let per_doc = 1 + usize::from(opts.with_scores) + usize::from(!opts.no_content);
   resp::write_array_header(out, 1 + page.len() * per_doc);
   resp::write_integer(out, total as i64);
   for (key, score) in page {
      resp::write_bulk(out, key);
      let score_str = format!("{score:.6}");
      if opts.with_scores {
         resp::write_bulk(out, &score_str);
      }
      if !opts.no_content {
         let mut fields = store
            .data
            .get_ref(key)
            .map(|v| doc_fields(&v, &def, &opts))
            .unwrap_or_default();
         fields.push(("__score".to_string(), score_str.into_bytes()));
         resp::write_array_header(out, fields.len() * 2);
         for (f, v) in &fields {
            resp::write_bulk(out, f);
            resp::write_bulk_bytes(out, v);
         }
      }
   }
}

fn lexical(
   store: &Store,
   def: &Arc<crate::search::IndexDef>,
   cond: Option<&crate::search::query::Cond>,
   window: usize,
) -> Vec<(String, f64)> {
   let Some(cond) = cond else { return Vec::new() };
   if let Some(fi) = store.search_state().filter_index(store, def)
      && let Some(res) = fi.resolve(def, cond)
   {
      let ids: Vec<u32> = match res {
         Resolved::All => (0..fi.doc_count() as u32).collect(),
         Resolved::Ids(ids) => ids,
      };
      let scores = fi
         .bm25(def, cond, &ids)
         .unwrap_or_else(|| vec![1.0; ids.len()]);
      let mut ranked: Vec<(u32, f32)> = ids.into_iter().zip(scores).collect();
      let cmp = |a: &(u32, f32), b: &(u32, f32)| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0));
      if ranked.len() > window {
         ranked.select_nth_unstable_by(window, cmp);
         ranked.truncate(window);
      }
      ranked.sort_unstable_by(cmp);
      return ranked
         .into_iter()
         .filter_map(|(d, s)| fi.key(d).map(|k| (k.to_string(), s as f64)))
         .collect();
   }
   let mut hits = Vec::new();
   store.data.for_each(|key, val| {
      if hits.len() < window
         && !val.is_expired()
         && def.matches_key(key)
         && doc_storage_matches(val, def)
         && cond_matches(cond, val, def)
      {
         hits.push((key.to_string(), 1.0));
      }
   });
   hits
}

fn vector(
   store: &Arc<Store>,
   def: &Arc<crate::search::IndexDef>,
   field: &str,
   attrs: &crate::search::VectorAttrs,
   qv: &[f32],
   window: usize,
   ef: usize,
) -> Option<Vec<(String, f64)>> {
   let entry = store.search_state().vector_index_entry(store, def, field)?;
   let graph = &entry.graph;
   let hits = if !entry.is_linked() {
      graph.brute_search(qv, window)
   } else {
      let depth = graph.rerank_depth(window, ef.max(window));
      let cands = graph.search(qv, depth, ef.max(depth));
      if graph.is_quantized() {
         let fetch = |id: usize| {
            entry
               .key_for_id(id)
               .and_then(|k| crate::search::graph::doc_vector(store, def, field, attrs, &k))
         };
         graph.rerank(qv, cands, window, &fetch)
      } else {
         cands.into_iter().take(window).collect()
      }
   };
   Some(
      hits
         .into_iter()
         .filter_map(|(d, id)| entry.key_for_id(id).map(|k| (k.to_string(), -(d as f64))))
         .collect(),
   )
}
