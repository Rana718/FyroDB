//! Attribute (tag/numeric) inverted index backing filtered `FT.SEARCH`.

use super::FieldType;
use super::StorageKind;
use super::FxHashMap;
use super::IndexDef;
use super::SearchState;
use crate::search::query::Cond;
use super::hooks::Drained;
use crate::storage::store::Store;
use std::sync::Arc;

// ── Attribute (filter) inverted index ────────────────────────────────────

/// Postings index over TAG/NUMERIC/TEXT fields. Turns a filtered
/// `FT.SEARCH` from a full keyspace scan (per-doc tag split + numeric
/// parse + per-term text split, ~2.5 ms at 50k docs) into a postings
/// intersection (~0.2 ms).
///
/// Maintained incrementally from the index's dirty keys: a rewritten or
/// deleted doc's old id becomes a tombstone and its new version gets a
/// fresh id appended to every posting it belongs to (so postings stay
/// ascending without re-sorting); results drop tombstones. Once they pass
/// `DEAD_REBUILD_RATIO` the index is rebuilt from a scan. Trusted only
/// while the store tracks no TTLs: lazy expiry removes docs without a write.
pub struct FilterIndex {
    ids: Vec<Option<Box<str>>>,
    by_key: FxHashMap<Box<str>, u32>,
    dead: usize,
    /// field idx → tag → doc ids, ascending.
    tags: FxHashMap<usize, FxHashMap<Box<str>, Vec<u32>>>,
    /// field idx → (value, doc id) sorted by value.
    nums: FxHashMap<usize, Vec<(f64, u32)>>,
    /// field idx → lowercased token → doc ids, ascending. Tokens mirror
    /// the scan's word split (non-alphanumeric boundaries, ASCII-case-
    /// insensitive compare), so a term resolves to exactly the docs the
    /// per-doc `text_match` would accept.
    texts: FxHashMap<usize, FxHashMap<Box<str>, Posting>>,
    /// field idx → token count per doc id (BM25 length normalization).
    doc_len: FxHashMap<usize, Vec<u32>>,
    /// field idx → summed token count over live docs.
    len_sum: FxHashMap<usize, u64>,
    /// Numeric fields holding NaN: sorted arrays can't place them the way
    /// the scan's partial_cmp does, so those fields fall back to the scan.
    nan_fields: Vec<usize>,
    tag_fields: Vec<(usize, char)>,
    num_fields: Vec<usize>,
    text_fields: Vec<usize>,
    /// Per vector field: doc id → vector id, tagged with the graph entry's
    /// epoch it was derived from. Turns the per-query key→id lookup of
    /// every matching doc into an array index.
    vid_maps: parking_lot::Mutex<FxHashMap<String, (u64, Arc<Vec<u32>>)>>,
}

#[derive(Default)]
pub struct Posting {
    docs: Vec<u32>,
    tf: Vec<u16>,
}

impl Posting {
    fn tf_of(&self, doc: u32) -> Option<u16> {
        self.docs.binary_search(&doc).ok().map(|i| self.tf[i])
    }
}

const BM25_K1: f32 = 1.2;
const BM25_B: f32 = 0.75;

/// Tombstones past this share of doc ids trigger a full rebuild.
const DEAD_REBUILD_RATIO: f64 = 0.25;

/// Result of resolving a condition against postings.
pub enum Resolved {
    /// Every indexed doc (bare `*`).
    All,
    /// Ascending, deduplicated doc ids.
    Ids(Vec<u32>),
}

impl FilterIndex {
    pub fn key(&self, id: u32) -> Option<&str> {
        self.ids.get(id as usize).and_then(|s| s.as_deref())
    }

    /// Live (non-tombstoned) documents.
    pub fn doc_count(&self) -> usize {
        self.ids.len() - self.dead
    }

    #[inline]
    fn alive(&self, id: u32) -> bool {
        self.ids.get(id as usize).is_some_and(|k| k.is_some())
    }

    /// Doc-id set for `cond` over live docs (see `resolve_raw`).
    pub fn resolve(&self, def: &IndexDef, cond: &Cond) -> Option<Resolved> {
        self.resolve_live(def, cond, true)
    }

    /// As `resolve`, but a top-level numeric range skips sorting its ids:
    /// for callers that only need membership (filtered KNN builds a bitset).
    pub fn resolve_unordered(&self, def: &IndexDef, cond: &Cond) -> Option<Resolved> {
        self.resolve_live(def, cond, false)
    }

    fn resolve_live(&self, def: &IndexDef, cond: &Cond, ordered: bool) -> Option<Resolved> {
        let r = match (cond, ordered) {
            (Cond::Numeric { .. }, false) => self.resolve_numeric(def, cond, false)?,
            _ => self.resolve_raw(def, cond)?,
        };
        if self.dead == 0 {
            return Some(r);
        }
        Some(Resolved::Ids(match r {
            Resolved::All => (0..self.ids.len() as u32).filter(|&i| self.alive(i)).collect(),
            Resolved::Ids(mut ids) => {
                ids.retain(|&i| self.alive(i));
                ids
            }
        }))
    }

    /// Doc id → vector id table for `field`, rebuilt only when this index
    /// or the graph entry's key map changed since it was derived.
    pub fn vids_for(&self, field: &str, entry: &crate::search::GraphEntry) -> Arc<Vec<u32>> {
        let epoch = entry.epoch();
        if let Some((e, m)) = self.vid_maps.lock().get(field)
            && *e == epoch
        {
            return Arc::clone(m);
        }
        let map = Arc::new(entry.ids_for_keys(self.ids.iter().map(|k| k.as_deref())));
        self.vid_maps.lock().insert(field.to_string(), (epoch, Arc::clone(&map)));
        map
    }

    fn needs_rebuild(&self) -> bool {
        self.dead > 1024 && self.dead as f64 > DEAD_REBUILD_RATIO * self.ids.len() as f64
    }

    /// Index one document under a fresh id (larger than every existing id,
    /// so tag/text postings stay ascending by plain push). Numeric entries
    /// go to `new_nums` for one sorted merge per delta.
    fn add_doc(&mut self, def: &IndexDef, key: &str, h: &crate::storage::value::HashInner, new_nums: &mut FxHashMap<usize, Vec<(f64, u32)>>) {
        let id = self.ids.len() as u32;
        self.ids.push(Some(key.into()));
        self.by_key.insert(key.into(), id);
        for &(fi, sep) in &self.tag_fields {
            let Some(raw) = h.get(&def.fields[fi].name) else {
                continue;
            };
            let mut toks: Vec<&str> = raw.as_str().split(sep).collect();
            toks.sort_unstable();
            toks.dedup();
            for t in &toks {
                self.tags.entry(fi).or_default().entry(Box::<str>::from(*t)).or_default().push(id);
            }
        }
        for &fi in &self.num_fields {
            if let Some(raw) = h.get(&def.fields[fi].name)
                && let Ok(v) = raw.as_str().parse::<f64>()
            {
                if v.is_nan() && !self.nan_fields.contains(&fi) {
                    self.nan_fields.push(fi);
                }
                new_nums.entry(fi).or_default().push((v, id));
            }
        }
        for &fi in &self.text_fields {
            let mut counts: FxHashMap<Box<str>, u16> = FxHashMap::default();
            let mut len = 0u32;
            if let Some(raw) = h.get(&def.fields[fi].name) {
                for t in super::tokens(raw.as_str()) {
                    len += 1;
                    let c = counts.entry(t.into()).or_insert(0);
                    *c = c.saturating_add(1);
                }
            }
            let lens = self.doc_len.entry(fi).or_default();
            lens.resize(id as usize, 0);
            lens.push(len);
            *self.len_sum.entry(fi).or_default() += len as u64;
            let postings = self.texts.entry(fi).or_default();
            for (t, c) in counts {
                let p = postings.entry(t).or_default();
                p.docs.push(id);
                p.tf.push(c);
            }
        }
    }

    fn merge_nums(&mut self, mut new_nums: FxHashMap<usize, Vec<(f64, u32)>>) {
        let cmp = |a: &(f64, u32), b: &(f64, u32)| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal);
        for (fi, mut add) in new_nums.drain() {
            add.sort_unstable_by(cmp);
            let old = self.nums.remove(&fi).unwrap_or_default();
            let mut merged = Vec::with_capacity(old.len() + add.len());
            let (mut i, mut j) = (0, 0);
            while i < old.len() && j < add.len() {
                if cmp(&old[i], &add[j]) != std::cmp::Ordering::Greater {
                    merged.push(old[i]);
                    i += 1;
                } else {
                    merged.push(add[j]);
                    j += 1;
                }
            }
            merged.extend_from_slice(&old[i..]);
            merged.extend_from_slice(&add[j..]);
            self.nums.insert(fi, merged);
        }
    }

    /// Re-index `keys` after writes: tombstone each key's old doc, index
    /// its current value (if it still belongs to the index).
    fn apply(&mut self, store: &Store, def: &IndexDef, keys: &[Box<str>]) {
        self.vid_maps.get_mut().clear();
        let mut new_nums: FxHashMap<usize, Vec<(f64, u32)>> = FxHashMap::default();
        for key in keys {
            if let Some(old) = self.by_key.remove(&**key) {
                self.ids[old as usize] = None;
                self.dead += 1;
                for (fi, lens) in &self.doc_len {
                    if let Some(&l) = lens.get(old as usize)
                        && let Some(sum) = self.len_sum.get_mut(fi)
                    {
                        *sum = sum.saturating_sub(l as u64);
                    }
                }
            }
            if !def.matches_key(key) {
                continue;
            }
            let this = &mut *self;
            store.data.read_consistent(key, |val| {
                if val.is_expired() {
                    return;
                }
                if let Some(h) = crate::storage::value::FyroDB::as_hash(&val.value) {
                    this.add_doc(def, key, h, &mut new_nums);
                }
            });
        }
        self.merge_nums(new_nums);
    }

    fn text_scope(&self, def: &IndexDef, field: Option<&str>) -> Option<Vec<usize>> {
        match field {
            Some(name) => Some(vec![self.field_idx(def, name, FieldType::Text)?]),
            None if !self.text_fields.is_empty() => Some(self.text_fields.clone()),
            None => None,
        }
    }

    fn term_docs(&self, scope: &[usize], term: &str) -> Vec<u32> {
        let mut acc: Vec<u32> = Vec::new();
        for fi in scope {
            if let Some(p) = self.texts.get(fi).and_then(|m| m.get(term)) {
                acc = union_ids(&acc, &p.docs);
            }
        }
        acc
    }

    /// Docs holding every (non-stopword) term within the scope's fields;
    /// Some(None) when all terms were stopwords (matches everything), None
    /// when the postings can't answer (globs, unindexed field).
    fn text_docs(&self, def: &IndexDef, field: Option<&str>, terms: &[String]) -> Option<Option<Vec<u32>>> {
        if terms.is_empty() || terms.iter().any(|t| t.contains('*')) {
            return None;
        }
        let scope = self.text_scope(def, field)?;
        let mut acc: Option<Vec<u32>> = None;
        for t in terms.iter().filter_map(|t| super::query_term(t)) {
            let docs = self.term_docs(&scope, &t);
            acc = Some(match acc {
                None => docs,
                Some(a) => intersect_ids(&a, &docs),
            });
            if acc.as_ref().is_some_and(|a| a.is_empty()) {
                break;
            }
        }
        Some(match acc {
            None => Some((0..self.ids.len() as u32).collect()),
            some => some,
        })
    }

    /// Positive text terms of `cond` with their field scope.
    fn scoring_terms(&self, def: &IndexDef, cond: &Cond, out: &mut Vec<(Vec<usize>, String)>) {
        match cond {
            Cond::Text { field, terms } => {
                if let Some(scope) = self.text_scope(def, field.as_deref()) {
                    for t in terms.iter().filter(|t| !t.contains('*')).filter_map(|t| super::query_term(t)) {
                        out.push((scope.clone(), t));
                    }
                }
            }
            Cond::And(a, b) | Cond::Or(a, b) => {
                self.scoring_terms(def, a, out);
                self.scoring_terms(def, b, out);
            }
            _ => {}
        }
    }

    /// BM25 score of each doc for the positive text terms in `cond`
    /// (Robertson/Spärck Jones idf, k1 = 1.2, b = 0.75), or None when the
    /// query has no text terms to rank by.
    pub fn bm25(&self, def: &IndexDef, cond: &Cond, docs: &[u32]) -> Option<Vec<f32>> {
        let mut terms = Vec::new();
        self.scoring_terms(def, cond, &mut terms);
        if terms.is_empty() {
            return None;
        }
        let n = self.doc_count().max(1) as f32;
        let mut scores = vec![0f32; docs.len()];
        for (scope, term) in &terms {
            for fi in scope {
                let Some(p) = self.texts.get(fi).and_then(|m| m.get(term.as_str())) else { continue };
                let df = p.docs.iter().filter(|&&d| self.alive(d)).count().max(1) as f32;
                let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).ln();
                let avgdl = (self.len_sum.get(fi).copied().unwrap_or(0) as f32 / n).max(1.0);
                let lens = self.doc_len.get(fi);
                for (s, &d) in scores.iter_mut().zip(docs) {
                    let Some(tf) = p.tf_of(d) else { continue };
                    let tf = tf as f32;
                    let dl = lens.and_then(|l| l.get(d as usize)).copied().unwrap_or(0) as f32;
                    *s += idf * tf * (BM25_K1 + 1.0) / (tf + BM25_K1 * (1.0 - BM25_B + BM25_B * dl / avgdl));
                }
            }
        }
        Some(scores)
    }

    fn field_idx(&self, def: &IndexDef, name: &str, want: FieldType) -> Option<usize> {
        def.fields
            .iter()
            .position(|f| (f.name == name || f.alias.as_deref() == Some(name)) && f.field_type == want && !f.no_index)
    }

    fn resolve_numeric(&self, def: &IndexDef, cond: &Cond, ordered: bool) -> Option<Resolved> {
        let Cond::Numeric { field, range } = cond else { return None };
        let fi = self.field_idx(def, field, FieldType::Numeric)?;
        if self.nan_fields.contains(&fi) {
            return None;
        }
        let sorted = self.nums.get(&fi)?;
        // partition_point wants true-then-false. `lo`: skip values
        // below the min. `hi`: keep values at/under the max.
        let lo = sorted.partition_point(|(v, _)| {
            if range.has_min {
                if range.min_excl { *v <= range.min } else { *v < range.min }
            } else {
                false
            }
        });
        let hi = sorted.partition_point(|(v, _)| {
            if range.has_max {
                if range.max_excl { *v < range.max } else { *v <= range.max }
            } else {
                true
            }
        });
        // An inverted range (min > max) counts lo past hi; the
        // scan answers "no docs" and so must the postings path.
        let hi = hi.max(lo);
        let mut ids: Vec<u32> = sorted[lo..hi].iter().map(|&(_, id)| id).collect();
        if ordered {
            ids.sort_unstable();
        }
        Some(Resolved::Ids(ids))
    }

    /// Doc-id set for `cond` (tombstones included), or None when the
    /// condition needs the exact scan: tag/text globs, fields the index
    /// doesn't cover, or numeric fields holding NaN (the scan matches NaN
    /// against bounded ranges; a sorted postings array cannot place it).
    fn resolve_raw(&self, def: &IndexDef, cond: &Cond) -> Option<Resolved> {
        match cond {
            Cond::MatchAll => Some(Resolved::All),
            Cond::MatchNone => Some(Resolved::Ids(Vec::new())),
            Cond::Tag { field, any } => {
                let fi = self.field_idx(def, field, FieldType::Tag)?;
                let mut acc: Vec<u32> = Vec::new();
                for t in any {
                    if t.contains('*') {
                        return None;
                    }
                    // Absent tag = no docs, not "can't answer".
                    if let Some(list) = self.tags.get(&fi).and_then(|m| m.get(t.as_str())) {
                        acc = union_ids(&acc, list);
                    }
                }
                Some(Resolved::Ids(acc))
            }
            Cond::Numeric { .. } => self.resolve_numeric(def, cond, true),
            Cond::Text { field, terms } => self.text_docs(def, field.as_deref(), terms).map(|d| Resolved::Ids(d.unwrap_or_default())),
            Cond::TextNeg { field, terms } => {
                let matched = self.text_docs(def, field.as_deref(), terms)?;
                Some(Resolved::Ids(complement_ids(&matched.unwrap_or_default(), self.ids.len())))
            }
            Cond::TagNeg { field, any } => {
                let fi = self.field_idx(def, field, FieldType::Tag)?;
                let tags = self.tags.get(&fi)?;
                if any.iter().any(|t| t.contains('*')) {
                    return None;
                }
                // Complement of "doc carries any of the tags": docs whose
                // tag set intersects `any` are excluded, field-less docs
                // kept — exactly the scan's `!hit`.
                let mut matched: Vec<u32> = Vec::new();
                for t in any {
                    if let Some(list) = tags.get(t.as_str()) {
                        matched = union_ids(&matched, list);
                    }
                }
                Some(Resolved::Ids(complement_ids(&matched, self.ids.len())))
            }
            Cond::And(a, b) => {
                let x = self.resolve_raw(def, a)?;
                let y = self.resolve_raw(def, b)?;
                match (x, y) {
                    (Resolved::All, r) | (r, Resolved::All) => Some(r),
                    (Resolved::Ids(x), Resolved::Ids(y)) => Some(Resolved::Ids(intersect_ids(&x, &y))),
                }
            }
            Cond::Or(a, b) => {
                let x = self.resolve_raw(def, a)?;
                let y = self.resolve_raw(def, b)?;
                match (x, y) {
                    (Resolved::All, _) | (_, Resolved::All) => Some(Resolved::All),
                    (Resolved::Ids(x), Resolved::Ids(y)) => Some(Resolved::Ids(union_ids(&x, &y))),
                }
            }
        }
    }
}

fn intersect_ids(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(a.len().min(b.len()));
    let (mut i, mut j) = (0usize, 0usize);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

fn union_ids(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0usize, 0usize);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                out.push(a[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(b[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out.extend_from_slice(&a[i..]);
    out.extend_from_slice(&b[j..]);
    out
}

/// Indexed docs not in `matched` (ascending, like every resolved set).
fn complement_ids(matched: &[u32], total: usize) -> Vec<u32> {
    if matched.is_empty() {
        return (0..total as u32).collect();
    }
    let mut present = vec![false; total];
    for &m in matched {
        if (m as usize) < total {
            present[m as usize] = true;
        }
    }
    (0..total as u32)
        .filter(|&i| !present[i as usize])
        .collect()
}

/// One keyspace scan extracting every TAG/NUMERIC/TEXT value. Mirrors the
/// scan predicates exactly: same separator split (no trimming, empty tokens
/// kept), same numeric parse, same word split and ASCII-case folding, same
/// key-prefix/type gating.
fn build_filter_index(store: &Store, def: &IndexDef) -> Option<FilterIndex> {
    if def.storage != StorageKind::Hash {
        return None;
    }
    let fields_of = |t: FieldType| -> Vec<usize> {
        def.fields
            .iter()
            .enumerate()
            .filter(|(_, f)| f.field_type == t && !f.no_index)
            .map(|(i, _)| i)
            .collect()
    };
    let mut fi = FilterIndex {
        ids: Vec::new(),
        by_key: FxHashMap::default(),
        dead: 0,
        tags: FxHashMap::default(),
        nums: FxHashMap::default(),
        texts: FxHashMap::default(),
        doc_len: FxHashMap::default(),
        len_sum: FxHashMap::default(),
        nan_fields: Vec::new(),
        tag_fields: fields_of(FieldType::Tag).into_iter().map(|i| (i, def.fields[i].separator)).collect(),
        num_fields: fields_of(FieldType::Numeric),
        text_fields: fields_of(FieldType::Text),
        vid_maps: Default::default(),
    };
    let mut new_nums: FxHashMap<usize, Vec<(f64, u32)>> = FxHashMap::default();
    store.data.for_each(|k, val| {
        if val.is_expired() || !def.matches_key(k) {
            return;
        }
        if let Some(h) = crate::storage::value::FyroDB::as_hash(&val.value) {
            fi.add_doc(def, k, h, &mut new_nums);
        }
    });
    fi.merge_nums(new_nums);
    Some(fi)
}

pub type FilterRead = parking_lot::ArcRwLockReadGuard<parking_lot::RawRwLock, FilterIndex>;

impl SearchState {
    /// Current postings for this index, first applying the keys written
    /// since the last query. None → caller must use the exact scan: JSON
    /// indexes, or TTL presence (lazy expiry removes docs without a write).
    pub fn filter_index(&self, store: &Store, def: &Arc<IndexDef>) -> Option<FilterRead> {
        if store.has_ttl_keys() {
            return None;
        }
        let dirty = self.hooks.get(&def.name)?;
        let existing = self.filters.lock().get(def.name.as_str()).cloned();
        if let Some(fi) = &existing
            && dirty.filter.is_clean()
        {
            return Some(fi.read_arc());
        }
        let _b = self.filter_build.lock();
        let existing = self.filters.lock().get(def.name.as_str()).cloned();
        let rebuild = match (&existing, dirty.filter.drain()) {
            (Some(fi), Drained::Keys(keys)) => {
                if !keys.is_empty() {
                    fi.write().apply(store, def, &keys);
                }
                fi.read().needs_rebuild()
            }
            _ => true,
        };
        if !rebuild {
            return existing.map(|fi| fi.read_arc());
        }
        // The drain above reset the set: writes landing during this scan
        // are marked again and applied by the next query.
        let fi = Arc::new(parking_lot::RwLock::new(build_filter_index(store, def)?));
        self.filters.lock().insert(def.name.clone(), Arc::clone(&fi));
        Some(fi.read_arc())
    }
}
