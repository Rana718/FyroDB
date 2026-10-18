use super::{DistanceMetric, FieldType, IndexDef};

/// Upper bounds for user-supplied KNN parameters preventing excessive
/// heap allocations on query paths.
pub const MAX_KNN_K: usize = 10_000;
pub const MAX_EF_RUNTIME: usize = 100_000;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Cmp {
    Lt,
    Gt,
    Le,
    Ge,
}

#[derive(Debug, Clone)]
pub struct NumericRange {
    pub min: f64,
    pub min_excl: bool,
    pub max: f64,
    pub max_excl: bool,
    pub has_min: bool,
    pub has_max: bool,
}

impl NumericRange {
    pub fn matches(&self, v: f64) -> bool {
        if self.has_min {
            if self.min_excl && v <= self.min {
                return false;
            }
            if !self.min_excl && v < self.min {
                return false;
            }
        }
        if self.has_max {
            if self.max_excl && v >= self.max {
                return false;
            }
            if !self.max_excl && v > self.max {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Clone)]
pub enum Cond {
    MatchAll,
    MatchNone,
    Numeric { field: String, range: NumericRange },
    Tag { field: String, any: Vec<String> },
    TagNeg { field: String, any: Vec<String> },
    Text { field: Option<String>, terms: Vec<String> },
    TextNeg { field: Option<String>, terms: Vec<String> },
    And(Box<Cond>, Box<Cond>),
    Or(Box<Cond>, Box<Cond>),
}

pub struct QueryContext<'a> {
    pub def: &'a IndexDef,
}

pub struct ParsedQuery {
    pub filter: Option<Cond>,
    pub knn: Option<KnnSpec>,
    pub range: Option<RangeSpec>,
}

pub struct KnnSpec {
    pub field: String,
    pub k: usize,
    pub param: String,
    pub as_field: String,
    pub ef_runtime: Option<usize>,
}

pub struct RangeSpec {
    pub field: String,
    pub radius_param: String,
    pub vector_param: String,
    pub as_field: String,
}

pub fn parse_query(def: &IndexDef, query: &str) -> Result<ParsedQuery, String> {
    let q = query.trim();
    if q.is_empty() || q == "*" {
        return Ok(ParsedQuery {
            filter: None,
            knn: None,
            range: None,
        });
    }

    if let Some(pos) = q.find("VECTOR_RANGE") {
        return parse_range_query(def, q, pos);
    }

    if let Some(eq) = q.find("=>") {
        let head = q[..eq].trim();
        let tail = &q[eq + 2..];
        let filter = if head == "*" || head.is_empty() {
            None
        } else {
            Some(parse_expr(def, head, 0).map_err(|e| format!("ERR {e}"))?)
        };
        let tail_trim = tail.trim();
        if tail_trim.starts_with('[') {
            let spec = parse_knn(def, tail_trim)?;
            return Ok(ParsedQuery {
                filter,
                knn: Some(spec),
                range: None,
            });
        }
        return Err("ERR syntax error after =>".to_string());
    }

    let filter = parse_expr(def, q, 0).map_err(|e| format!("ERR {e}"))?;
    Ok(ParsedQuery {
        filter: Some(filter),
        knn: None,
        range: None,
    })
}

// VECTOR_RANGE split: the closing paren of a wrapper opened before @field ends up
// outside the filter head, so drop one unmatched opener.
fn strip_unclosed_wrapper(fstr: &str) -> &str {
    let b = fstr.as_bytes();
    if b.first() != Some(&b'(') {
        return fstr;
    }
    let mut depth = 0i32;
    let mut i = 0usize;
    while i < b.len() {
        match b[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'{' => {
                while i < b.len() && b[i] != b'}' {
                    i += 1;
                }
            }
            b'[' => {
                while i < b.len() && b[i] != b']' {
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    if depth > 0 { &fstr[1..] } else { fstr }
}

fn parse_range_query(
    def: &IndexDef,
    q: &str,
    marker: usize,
) -> Result<ParsedQuery, String> {
    let head = &q[..marker];
    let field_start = head
        .rfind('@')
        .map(|p| p + 1)
        .ok_or("ERR missing vector field")?;
    // Anything before the vector @field is the filter; previously dropped, causing range
    // queries to ignore tag predicates silently.
    let filter = {
        let fstr = strip_unclosed_wrapper(q[..field_start - 1].trim());
        if fstr.is_empty() || fstr == "*" {
            None
        } else {
            Some(parse_expr(def, fstr, 0).map_err(|e| format!("ERR {e}"))?)
        }
    };
    let colon = head[field_start..]
        .find(':')
        .map(|p| field_start + p)
        .ok_or("ERR missing vector field separator")?;
    let field = q[field_start..colon].trim().to_string();
    // The `=>{$YIELD_DISTANCE_AS: …}` attribute block is optional.
    let body_start = colon + 1;
    let body_end = q[body_start..].find(']').map(|p| body_start + p + 1).ok_or("ERR bad range syntax")?;
    let body = &q[body_start..body_end];
    let spec = parse_range_body(def, &field, body)?;
    let mut as_field = "__vector_distance".to_string();
    if let Some(y) = q.find("YIELD_DISTANCE_AS") {
        let rest = &q[y + "YIELD_DISTANCE_AS".len()..];
        let val = rest.trim_start_matches([':', ' ']);
        if let Some(end) = val.find(['}', ';']) {
            as_field = val[..end].trim().to_string();
        }
    }
    Ok(ParsedQuery {
        filter,
        knn: None,
        range: Some(RangeSpec {
            field: spec.field,
            radius_param: spec.radius_param,
            vector_param: spec.vector_param,
            as_field,
        }),
    })
}

fn parse_knn(def: &IndexDef, body: &str) -> Result<KnnSpec, String> {
    let inner = body
        .strip_prefix('[')
        .and_then(|b| b.strip_suffix(']'))
        .ok_or("ERR bad KNN syntax")?;
    let toks: Vec<&str> = tokenize(inner)?;
    let mut i = 0;
    if toks.get(i).map(|t| t.to_ascii_uppercase()).as_deref() != Some("KNN") {
        return Err("ERR expected KNN".into());
    }
    i += 1;
    let k: usize = toks
        .get(i)
        .and_then(|t| t.parse().ok())
        .ok_or("ERR bad KNN count")?;
    // k sizes the result heaps; without a bound a single query with
    // k = usize::MAX would try to preallocate it and abort the process.
    if k == 0 || k > MAX_KNN_K {
        return Err(format!("ERR KNN is only supported for 1 <= K <= {MAX_KNN_K}"));
    }
    i += 1;
    let field = toks
        .get(i)
        .ok_or("ERR missing KNN field")?
        .trim_start_matches('@')
        .to_string();
    i += 1;
    let param = toks.get(i).ok_or("ERR missing KNN vector param")?.to_string();
    i += 1;
    let mut as_field = "__vector_score".to_string();
    let mut ef_runtime = None;
    while i < toks.len() {
        match toks[i].to_ascii_uppercase().as_str() {
            "AS" => {
                as_field = toks.get(i + 1).ok_or("ERR AS requires a name")?.to_string();
                i += 2;
            }
            "EF_RUNTIME" => {
                // Unparseable keeps the lenient default 10; only explicit
                // out-of-bound values are rejected.
                let ef: usize = toks
                    .get(i + 1)
                    .and_then(|t| t.parse().ok())
                    .unwrap_or(10);
                if ef == 0 || ef > MAX_EF_RUNTIME {
                    return Err(format!(
                        "ERR EF_RUNTIME is only supported for 1 <= EF <= {MAX_EF_RUNTIME}"
                    ));
                }
                ef_runtime = Some(ef);
                i += 2;
            }
            "HYBRID_POLICY" | "BATCH_SIZE" => i += 2,
            _ => i += 1,
        }
    }
    if def.field(&field).is_none() {
        return Err(format!("ERR Unknown field {field}"));
    }
    Ok(KnnSpec {
        field,
        k,
        param,
        as_field,
        ef_runtime,
    })
}

pub fn parse_range_body(def: &IndexDef, field: &str, body: &str) -> Result<RangeSpec, String> {
    let inner = body
        .strip_prefix('[')
        .and_then(|b| b.strip_suffix(']'))
        .ok_or("ERR bad range syntax")?;
    let toks: Vec<&str> = tokenize(inner)?;
    if toks.first().map(|t| t.eq_ignore_ascii_case("VECTOR_RANGE")) != Some(true) {
        return Err("ERR expected VECTOR_RANGE".into());
    }
    if toks.len() < 3 {
        return Err("ERR VECTOR_RANGE requires radius and vector".into());
    }
    let radius_param = toks[1].to_string();
    let vector_param = toks[2].to_string();
    if def.field(field).is_none() {
        return Err(format!("ERR Unknown field {field}"));
    }
    Ok(RangeSpec {
        field: field.to_string(),
        radius_param,
        vector_param,
        as_field: "__vector_score".to_string(),
    })
}

fn tokenize(s: &str) -> Result<Vec<&str>, String> {
    let mut toks = Vec::new();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b' ' | b'\t' => i += 1,
            b'"' => {
                let start = i + 1;
                let mut j = start;
                while j < bytes.len() && bytes[j] != b'"' {
                    j += 1;
                }
                if j >= bytes.len() {
                    return Err("ERR unterminated quote".into());
                }
                toks.push(&s[start..j]);
                i = j + 1;
            }
            _ => {
                let start = i;
                while i < bytes.len() && bytes[i] != b' ' && bytes[i] != b'\t' {
                    i += 1;
                }
                toks.push(&s[start..i]);
            }
        }
    }
    Ok(toks)
}

fn skip_space(s: &str, mut i: usize) -> usize {
    let b = s.as_bytes();
    while i < b.len() && (b[i] == b' ' || b[i] == b'\t') {
        i += 1;
    }
    i
}

fn parse_expr(def: &IndexDef, s: &str, depth: usize) -> Result<Cond, String> {
    if depth > 64 {
        return Err("query nesting too deep".into());
    }
    let mut clauses: Vec<(bool, Cond)> = Vec::new();
    let mut i = skip_space(s, 0);
    let b = s.as_bytes();

    loop {
        if i >= b.len() {
            break;
        }
        if b[i] == b'(' {
            let mut depth_p = 1;
            let mut j = i + 1;
            while j < b.len() && depth_p > 0 {
                match b[j] {
                    b'(' => depth_p += 1,
                    b')' => depth_p -= 1,
                    b'[' => {
                        while j < b.len() && b[j] != b']' {
                            j += 1;
                        }
                    }
                    b'{' => {
                        while j < b.len() && b[j] != b'}' {
                            j += 1;
                        }
                    }
                    _ => {}
                }
                j += 1;
            }
            if depth_p != 0 {
                return Err("unbalanced parens".into());
            }
            let inner = &s[i + 1..j - 1];
            let neg = peek_neg_prefix(s, i);
            let cond = parse_expr(def, inner, depth + 1)?;
            clauses.push((false, apply_neg(cond, neg)));
            i = skip_space(s, j);
        } else {
            let clause_start = i;
            let mut j = i;
            while j < b.len() && b[j] != b' ' && b[j] != b'\t' {
                if b[j] == b'(' || b[j] == b')' {
                    break;
                }
                j += 1;
            }
            if j == clause_start {
                return Err("syntax error".into());
            }
            let token = &s[clause_start..j];
            let neg = token == "-";
            if neg {
                i = skip_space(s, j);
                continue;
            }
            let (cond, consumed) = parse_clause(def, s, i, depth)?;
            clauses.push((false, cond));
            i = skip_space(s, consumed);
        }

        if i < b.len() && b[i] == b'|' {
            let last = clauses.last_mut();
            if let Some(c) = last {
                c.0 = true;
            }
            i = skip_space(s, i + 1);
        }
    }

    match clauses.len() {
        0 => Ok(Cond::MatchAll),
        1 => Ok(clauses.pop().map(|(_, c)| c).unwrap()),
        _ => {
            let mut result = clauses.remove(0).1;
            for (is_or, cond) in clauses {
                result = if is_or {
                    Cond::Or(Box::new(result), Box::new(cond))
                } else {
                    Cond::And(Box::new(result), Box::new(cond))
                };
            }
            Ok(result)
        }
    }
}

fn peek_neg_prefix(_s: &str, _i: usize) -> bool {
    false
}

fn apply_neg(cond: Cond, neg: bool) -> Cond {
    if !neg {
        return cond;
    }
    match cond {
        Cond::Tag { field, any } => Cond::TagNeg { field, any },
        Cond::Text { field, terms } => Cond::TextNeg { field, terms },
        Cond::And(a, b) => Cond::And(
            Box::new(apply_neg(*a, true)),
            Box::new(apply_neg(*b, true)),
        ),
        Cond::Or(a, b) => Cond::Or(
            Box::new(apply_neg(*a, true)),
            Box::new(apply_neg(*b, true)),
        ),
        other => other,
    }
}

fn parse_clause(def: &IndexDef, s: &str, start: usize, depth: usize) -> Result<(Cond, usize), String> {
    let b = s.as_bytes();
    let mut i = start;
    let mut neg = false;
    if i < b.len() && b[i] == b'-' {
        neg = true;
        i = skip_space(s, i + 1);
    }
    if i >= b.len() {
        return Ok((Cond::MatchAll, i));
    }

    let field_start = i;
    while i < b.len() && b[i] != b':' && b[i] != b' ' && b[i] != b'\t' {
        i += 1;
    }
    if i >= b.len() || b[i] != b':' {
        let term = &s[field_start..i];
        let cond = text_cond(None, term);
        return Ok((apply_neg(cond, neg), i));
    }

    let field_name = s[field_start..i].trim_start_matches('@');
    i += 1;
    let Some(field) = def.field(field_name) else {
        return Err(format!("Unknown field {field_name}"));
    };

    match field.field_type {
        FieldType::Numeric => {
            let (range, consumed) = parse_numeric_range(s, i)?;
            Ok((
                apply_neg(
                    Cond::Numeric {
                        field: field.name.clone(),
                        range,
                    },
                    neg,
                ),
                consumed,
            ))
        }
        FieldType::Tag => {
            let (values, consumed) = parse_tag_values(s, i)?;
            Ok((
                apply_neg(
                    Cond::Tag {
                        field: field.name.clone(),
                        any: values,
                    },
                    neg,
                ),
                consumed,
            ))
        }
        FieldType::Text => {
            let (cond, consumed) = parse_text_cond(s, i, &field.name)?;
            Ok((apply_neg(cond, neg), consumed))
        }
        FieldType::Vector => {
            let consumed = parse_vector_clause_tail(s, i)?;
            let _ = depth;
            Ok((Cond::MatchAll, consumed))
        }
    }
}

fn parse_vector_clause_tail(s: &str, i: usize) -> Result<usize, String> {
    let b = s.as_bytes();
    let mut j = i;
    if j < b.len() && b[j] == b'[' {
        // Depth counts from the opener at `i`; stop on its match.
        let mut depth = 0usize;
        while j < b.len() {
            match b[j] {
                b'[' => depth += 1,
                b']' => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            j += 1;
        }
        j = (j + 1).min(s.len());
    }
    Ok(j)
}

fn parse_numeric_range(s: &str, i: usize) -> Result<(NumericRange, usize), String> {
    let b = s.as_bytes();
    let mut j = i;
    if j >= b.len() || b[j] != b'[' {
        return Err("expected [ for numeric range".into());
    }
    j += 1;
    let start = j;
    while j < b.len() && b[j] != b']' {
        j += 1;
    }
    if j >= b.len() {
        return Err("unterminated numeric range".into());
    }
    let body = &s[start..j];
    let mut parts = body.split_whitespace();
    let min_tok = parts.next().ok_or("empty numeric range")?;
    let max_tok = parts.next().ok_or("numeric range needs two values")?;
    let mut range = NumericRange {
        min: f64::NEG_INFINITY,
        min_excl: false,
        max: f64::INFINITY,
        max_excl: false,
        has_min: true,
        has_max: true,
    };
    if min_tok != "inf" && min_tok != "+inf" && min_tok != "-inf" {
        let (excl, tok) = match min_tok.strip_prefix('(') {
            Some(t) => (true, t.strip_suffix(')').unwrap_or(t)),
            None => (false, min_tok),
        };
        range.min = tok.parse().map_err(|_| format!("bad numeric min {tok}"))?;
        range.min_excl = excl;
    } else if min_tok == "-inf" {
        range.has_min = false;
    }
    if max_tok != "inf" && max_tok != "+inf" && max_tok != "-inf" {
        let (excl, tok) = match max_tok.strip_prefix('(') {
            Some(t) => (true, t.strip_suffix(')').unwrap_or(t)),
            None => (false, max_tok),
        };
        range.max = tok.parse().map_err(|_| format!("bad numeric max {tok}"))?;
        range.max_excl = excl;
    } else if max_tok == "+inf" || max_tok == "inf" {
        range.has_max = false;
    }
    Ok((range, j + 1))
}

fn parse_tag_values(s: &str, i: usize) -> Result<(Vec<String>, usize), String> {
    let b = s.as_bytes();
    let mut j = i;
    if j >= b.len() || b[j] != b'{' {
        return Err("expected { for tag values".into());
    }
    j += 1;
    let start = j;
    while j < b.len() && b[j] != b'}' {
        j += 1;
    }
    if j >= b.len() {
        return Err("unterminated tag values".into());
    }
    let body = &s[start..j];
    let mut values = Vec::new();
    for alt in body.split('|') {
        let v = alt.replace("\\,", ",").replace("\\|", "|");
        values.push(v);
    }
    Ok((values, j + 1))
}

fn parse_text_cond(s: &str, i: usize, field: &str) -> Result<(Cond, usize), String> {
    let b = s.as_bytes();
    if i < b.len() && b[i] == b'(' {
        // Find the matching ')': depth counts from the opener at `i`.
        let mut depth = 0usize;
        let mut j = i;
        while j < b.len() {
            match b[j] {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            j += 1;
        }
        let inner = &s[i + 1..j.min(s.len())];
        let mut alts: Vec<Cond> = Vec::new();
        for part in inner.split('|') {
            let terms: Vec<String> = part
                .split_whitespace()
                .map(|t| t.trim_matches('"').to_string())
                .filter(|t| !t.is_empty())
                .collect();
            if !terms.is_empty() {
                alts.push(Cond::Text {
                    field: Some(field.to_string()),
                    terms,
                });
            }
        }
        let cond = match alts.len() {
            0 => Cond::MatchNone,
            1 => alts.pop().unwrap(),
            _ => {
                let mut it = alts.into_iter();
                let mut cond = it.next().unwrap();
                for c in it {
                    cond = Cond::Or(Box::new(cond), Box::new(c));
                }
                cond
            }
        };
        return Ok((cond, (j + 1).min(s.len())));
    }
    let mut j = i;
    while j < b.len() && b[j] != b' ' && b[j] != b'\t' {
        j += 1;
    }
    let term = s[i..j].trim_matches('"');
    Ok((
        Cond::Text {
            field: Some(field.to_string()),
            terms: vec![term.to_string()],
        },
        j,
    ))
}

fn text_cond(field: Option<String>, term: &str) -> Cond {
    Cond::Text { field, terms: vec![term.to_string()] }
}

pub fn numeric_match(range: &NumericRange, v: f64) -> bool {
    range.matches(v)
}

pub fn metric_for(def: &IndexDef, field: &str) -> Option<DistanceMetric> {
    def.field(field)
        .and_then(|f| f.vector.as_ref())
        .map(|v| v.metric)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::{FieldDef, FieldType, StorageKind, VectorAttrs, DistanceMetric};

    fn test_def() -> IndexDef {
        IndexDef {
            name: "t".into(),
            prefixes: vec!["doc:".into()],
            storage: StorageKind::Hash,
            created_at_ms: 0,
            num_docs: std::sync::atomic::AtomicU64::new(0),
            fields: vec![
                FieldDef { name: "title".into(), alias: None, field_type: FieldType::Text, sortable: false, no_index: false, separator: ',', vector: None },
                FieldDef { name: "category".into(), alias: None, field_type: FieldType::Tag, sortable: false, no_index: false, separator: ',', vector: None },
                FieldDef { name: "views".into(), alias: None, field_type: FieldType::Numeric, sortable: false, no_index: false, separator: ',', vector: None },
                FieldDef { name: "embedding".into(), alias: None, field_type: FieldType::Vector, sortable: false, no_index: false, separator: ',', vector: Some(VectorAttrs { dims: 4, metric: DistanceMetric::Cosine, algorithm: "FLAT", dtype: "FLOAT32", m: 16, ef_construction: 100, quant: fyro_vector::Quant::None }) },
            ],
        }
    }

    #[test]
    fn parses_tag_and_numeric_intersection() {
        let def = test_def();
        let pq = parse_query(&def, "@category:{tech} @views:[50 200]").unwrap();
        assert!(pq.filter.is_some());
    }

    #[test]
    fn parses_knn() {
        let def = test_def();
        let pq = parse_query(&def, "*=>[KNN 3 @embedding $vec AS score]").unwrap();
        assert_eq!(pq.knn.as_ref().unwrap().k, 3);
        assert_eq!(pq.knn.as_ref().unwrap().as_field, "score");
    }

    #[test]
    fn parses_filtered_knn() {
        let def = test_def();
        let pq = parse_query(&def, "(@category:{tech})=>[KNN 3 @embedding $vec AS score]").unwrap();
        assert!(pq.filter.is_some());
        assert!(pq.knn.is_some());
    }

    #[test]
    fn parses_range_query() {
        let def = test_def();
        let pq = parse_query(&def, "@embedding:[VECTOR_RANGE $r $v]=>{$YIELD_DISTANCE_AS: dist}").unwrap();
        assert!(pq.range.is_some());
        assert_eq!(pq.range.as_ref().unwrap().as_field, "dist");
    }

    #[test]
    fn parses_negation_and_union() {
        let def = test_def();
        let pq = parse_query(&def, "(-@category:{a} | @category:{b})").unwrap();
        assert!(pq.filter.is_some());
    }
}
