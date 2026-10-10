mod aggregate;
mod hybrid;
mod doc;
mod index;
mod search;

use index::AliasOp;
use crate::search::SEARCH_MODULE_VERSION;
use crate::storage::store::Store;
use crate::utils::resp;
use std::sync::Arc;

pub fn execute_ft(parts: &[&str], store: &std::sync::Arc<Store>, out: &mut Vec<u8>) {
    let sub = parts.first().copied().unwrap_or("");
    match sub.to_ascii_uppercase().as_str() {
        "FT.CREATE" => index::ft_create(parts, store, out),
        "FT.INFO" => index::ft_info(parts, store, out),
        "FT.DROPINDEX" => index::ft_dropindex(parts, store, out),
        "FT.ALTER" => index::ft_alter(parts, store, out),
        "FT.ALIASADD" => index::ft_alias(parts, store, out, AliasOp::Add),
        "FT.ALIASUPDATE" => index::ft_alias(parts, store, out, AliasOp::Update),
        "FT.ALIASDEL" => index::ft_alias(parts, store, out, AliasOp::Del),
        "FT.ALIASLIST" => index::ft_aliaslist(store, out),
        "FT.SEARCH" => search::ft_search(parts, store, out),
        "FT.AGGREGATE" => aggregate::ft_aggregate(parts, store, out),
        "FT.CURSOR" => aggregate::ft_cursor(parts, store, out),
        "FT.CONFIG" => index::ft_config(parts, out),
        _ => resp::write_store_err(out, "ERR unknown subcommand"),
    }
}

pub fn module_list(out: &mut Vec<u8>) {
    resp::write_array_header(out, 1);
    resp::write_array_header(out, 4);
    resp::write_bulk(out, "name");
    resp::write_bulk(out, "search");
    resp::write_bulk(out, "ver");
    resp::write_integer(out, SEARCH_MODULE_VERSION);
}

pub fn search_dispatch(
    parts: &[&str],
    binaries: &[(usize, Vec<u8>)],
    store: &Arc<Store>,
    out: &mut Vec<u8>,
) {
    if parts.first().is_some_and(|c| c.eq_ignore_ascii_case("FT.HYBRID")) {
        return hybrid::ft_hybrid(parts, binaries, store, out);
    }
    search::ft_search_impl(parts, binaries, store, out)
}
#[cfg(test)]
mod tests {
    use super::*;

    fn f32le(xs: &[f32]) -> Vec<u8> {
        xs.iter().flat_map(|x| x.to_le_bytes()).collect()
    }

    // Regression: filtered KNN must not return docs that fail the tag predicate.
    #[test]
    fn filtered_knn_respects_tag_predicate() {
        let store = Arc::new(Store::with_config(4, usize::MAX));
        let mut out = Vec::new();
        execute_ft(
            &[
                "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "d:", "SCHEMA", "cat", "TAG",
                "e", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC",
                "L2",
            ],
            &store,
            &mut out,
        );

        // near the query (0,0) is `far` (tag x); `near` carries tag y.
        store
            .hset_bytes("d:near", &[("cat", "y".as_bytes()), ("e", &f32le(&[0.0, 0.0]))])
            .unwrap();
        store
            .hset_bytes("d:far", &[("cat", "x".as_bytes()), ("e", &f32le(&[9.0, 9.0]))])
            .unwrap();

        let q = f32le(&[0.1, 0.1]);
        let run = |query: &str| {
            let parts = [
                "FT.SEARCH", "idx", query, "LIMIT", "0", "10", "NOCONTENT", "PARAMS", "2", "v",
                "",
            ];
            let mut o = Vec::new();
            search_dispatch(&parts, &[(10usize, q.clone())], &store, &mut o);
            String::from_utf8_lossy(&o).into_owned()
        };

        // Unfiltered: the nearest vector (`d:near`) wins.
        let plain = run("*=>[KNN 2 @e $v AS s]");
        assert!(plain.contains("d:near"), "unfiltered missing near: {plain}");

        // Filtered to cat:{x}: only `d:far` qualifies even though it is the
        // farther vector — the predicate, not the distance, decides.
        let filt = run("(@cat:{x})=>[KNN 2 @e $v AS s]");
        assert!(filt.contains("d:far"), "filtered missing far: {filt}");
        assert!(!filt.contains("d:near"), "filter leaked near: {filt}");
    }

    fn create_idx(store: &Arc<Store>) {
        let mut out = Vec::new();
        execute_ft(
            &[
                "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "d:", "SCHEMA", "title",
                "TEXT", "cat", "TAG", "views", "NUMERIC", "e", "VECTOR", "FLAT", "6", "TYPE",
                "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2",
            ],
            store,
            &mut out,
        );
    }

    // The postings fast path's partition_points run lo past hi on an
    // inverted range; the scan answers "no docs", and before the clamp
    // this aborted the whole process (panic=abort).
    #[test]
    fn inverted_numeric_range_returns_empty() {
        let store = Arc::new(Store::with_config(4, usize::MAX));
        create_idx(&store);
        for (k, views) in [("d:1", "10"), ("d:2", "30"), ("d:3", "50")] {
            store
                .hset_bytes(k, &[("cat", b"a"), ("views", views.as_bytes())])
                .unwrap();
        }
        let mut out = Vec::new();
        execute_ft(
            &["FT.SEARCH", "idx", "@views:[40 20]", "LIMIT", "0", "10", "NOCONTENT"],
            &store,
            &mut out,
        );
        let s = String::from_utf8_lossy(&out).into_owned();
        assert!(s.starts_with("*1\r\n:0\r\n"), "inverted range must be empty: {s}");
    }

    // Text terms resolve through token postings: same docs the per-doc
    // scan's word split + case folding would return.
    #[test]
    fn text_postings_match_scan_semantics() {
        let store = Arc::new(Store::with_config(4, usize::MAX));
        create_idx(&store);
        let docs = [
            ("d:1", "Doc Alpha"),
            ("d:2", "doc beta"),
            ("d:3", "gamma alpha delta"),
            ("d:4", "alphaic"), // substring, must NOT match "alpha"
        ];
        for (k, title) in docs {
            store.hset_bytes(k, &[("title", title.as_bytes())]).unwrap();
        }
        let search = |query: &str| {
            let mut o = Vec::new();
            execute_ft(
                &["FT.SEARCH", "idx", query, "LIMIT", "0", "10", "NOCONTENT"],
                &store,
                &mut o,
            );
            String::from_utf8_lossy(&o).into_owned()
        };

        // Case-insensitive term match.
        let one = search("@title:ALPHA");
        assert!(one.contains("d:1") && one.contains("d:3"), "term: {one}");
        assert!(!one.contains("d:2") && !one.contains("d:4"), "term leaked: {one}");

        // Multi-term AND (paren form).
        let both = search("@title:(doc alpha)");
        assert!(both.contains("d:1"), "and-term: {both}");
        assert!(!both.contains("d:3"), "and-term leaked: {both}");

        // Negation is the complement (docs without the term).
        let neg = search("-@title:alpha");
        assert!(neg.contains("d:2") && neg.contains("d:4"), "negation: {neg}");
        assert!(!neg.contains("d:1") && !neg.contains("d:3"), "negation leaked: {neg}");

        // Text + tag intersect through mixed postings.
        store.hset_bytes("d:1", &[("cat", b"x")]).unwrap();
        let mixed = search("(@title:alpha @cat:{x})");
        assert!(mixed.contains("d:1") && !mixed.contains("d:3"), "mixed: {mixed}");
    }

    // VECTOR_RANGE drops documents outside its filter clause — the parser
    // used to discard the head expression entirely.
    #[test]
    fn vector_range_respects_filter() {
        let store = Arc::new(Store::with_config(4, usize::MAX));
        create_idx(&store);
        store
            .hset_bytes("d:a", &[("cat", b"x"), ("e", &f32le(&[0.0, 0.0]))])
            .unwrap();
        store
            .hset_bytes("d:b", &[("cat", b"y"), ("e", &f32le(&[0.1, 0.1]))])
            .unwrap();
        let q = f32le(&[0.0, 0.0]);
        let parts = [
            "FT.SEARCH", "idx", "(@cat:{x} @e:[VECTOR_RANGE $r $v]=>{$YIELD_DISTANCE_AS: d})",
            "LIMIT", "0", "10", "NOCONTENT", "PARAMS", "4", "r", "1.5", "v", "",
        ];
        let mut o = Vec::new();
        search_dispatch(&parts, &[(12usize, q.clone())], &store, &mut o);
        let s = String::from_utf8_lossy(&o).into_owned();
        assert!(s.contains("d:a"), "filtered range missing d:a: {s}");
        assert!(!s.contains("d:b"), "filtered range leaked d:b: {s}");
    }

    // k sizes the result heaps; usize::MAX used to abort the process on
    // the capacity preallocation.
    #[test]
    fn knn_rejects_absurd_k() {
        let store = Arc::new(Store::with_config(4, usize::MAX));
        create_idx(&store);
        store
            .hset_bytes("d:1", &[("e", &f32le(&[0.0, 0.0]))])
            .unwrap();
        let mut out = Vec::new();
        execute_ft(
            &[
                "FT.SEARCH", "idx", "*=>[KNN 18446744073709551615 @e $v AS s]", "LIMIT", "0",
                "10", "NOCONTENT",
            ],
            &store,
            &mut out,
        );
        let s = String::from_utf8_lossy(&out).into_owned();
        assert!(s.contains("ERR"), "absurd k must error, got: {s}");
    }
}
