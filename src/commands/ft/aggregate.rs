use super::doc::{SearchOpts, cond_matches, doc_fields, doc_storage_matches};
use crate::search::query::parse_query;
use crate::search::{IndexDef, Resolved};
use crate::storage::store::Store;
use crate::storage::value::StoreValue;
use crate::utils::resp;
use std::collections::{HashMap, HashSet};

type Row = Vec<(String, Option<Vec<u8>>)>;
type ListMap = HashMap<usize, Vec<(String, Vec<Vec<u8>>)>>;
type ListSlice<'a> = Option<&'a [(String, Vec<Vec<u8>>)]>;

#[derive(Clone, Copy, PartialEq)]
enum Func {
    Count,
    CountDistinct,
    Sum,
    Avg,
    Min,
    Max,
    ToList,
}

struct Reducer {
    func: Func,
    arg: Option<String>,
    alias: String,
}

enum Acc {
    Count(u64),
    Distinct(HashSet<Vec<u8>>),
    Sum(f64),
    Avg(f64, u64),
    Min(Option<f64>),
    Max(Option<f64>),
    List(Vec<Vec<u8>>, HashSet<Vec<u8>>),
}

impl Reducer {
    fn init(&self) -> Acc {
        match self.func {
            Func::Count => Acc::Count(0),
            Func::CountDistinct => Acc::Distinct(HashSet::new()),
            Func::Sum => Acc::Sum(0.0),
            Func::Avg => Acc::Avg(0.0, 0),
            Func::Min => Acc::Min(None),
            Func::Max => Acc::Max(None),
            Func::ToList => Acc::List(Vec::new(), HashSet::new()),
        }
    }
}

impl Acc {
    fn add(&mut self, v: Option<&[u8]>) {
        let num = || {
            v.and_then(|b| std::str::from_utf8(b).ok())
                .and_then(|s| s.parse::<f64>().ok())
        };
        match self {
            Acc::Count(n) => *n += 1,
            Acc::Distinct(set) => {
                if let Some(b) = v {
                    set.insert(b.to_vec());
                }
            }
            Acc::Sum(s) => *s += num().unwrap_or(0.0),
            Acc::Avg(s, n) => {
                if let Some(x) = num() {
                    *s += x;
                    *n += 1;
                }
            }
            Acc::Min(m) => {
                if let Some(x) = num() {
                    *m = Some(m.map_or(x, |c| c.min(x)));
                }
            }
            Acc::Max(m) => {
                if let Some(x) = num() {
                    *m = Some(m.map_or(x, |c| c.max(x)));
                }
            }
            Acc::List(items, seen) => {
                if let Some(b) = v
                    && seen.insert(b.to_vec())
                {
                    items.push(b.to_vec());
                }
            }
        }
    }

    fn finish(&self) -> Value {
        let f = |x: f64| Value::Bulk(crate::utils::util::format_float(x).into_bytes());
        match self {
            Acc::Count(n) => f(*n as f64),
            Acc::Distinct(s) => f(s.len() as f64),
            Acc::Sum(s) => f(*s),
            Acc::Avg(s, n) => f(if *n == 0 { 0.0 } else { s / *n as f64 }),
            Acc::Min(m) => m.map_or(Value::Nil, f),
            Acc::Max(m) => m.map_or(Value::Nil, f),
            Acc::List(items, _) => Value::List(items.clone()),
        }
    }
}

enum Value {
    Nil,
    Bulk(Vec<u8>),
    List(Vec<Vec<u8>>),
}

struct Plan {
    load: Vec<String>,
    load_all: bool,
    group: Option<(Vec<String>, Vec<Reducer>)>,
    sort: Vec<(String, bool)>,
    sort_max: Option<usize>,
    limit: Option<(usize, usize)>,
    cursor: Option<usize>,
}

fn parse_plan(args: &[&str]) -> Result<Plan, String> {
    let mut plan = Plan {
        load: Vec::new(),
        load_all: true,
        group: None,
        sort: Vec::new(),
        sort_max: None,
        limit: None,
        cursor: None,
    };
    let count = |i: usize| -> Result<usize, String> {
        args.get(i)
            .and_then(|v| v.parse::<usize>().ok())
            .ok_or_else(|| format!("bad count after {}", args[i - 1]))
    };
    let prop = |s: &str| s.trim_start_matches('@').to_string();
    let mut i = 0;
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_str() {
            "LOAD" => {
                if args.get(i + 1) == Some(&"*") {
                    i += 2;
                    continue;
                }
                let n = count(i + 1)?;
                plan.load_all = n == 0;
                plan.load
                    .extend(args.iter().skip(i + 2).take(n).map(|s| prop(s)));
                i += 2 + n;
            }
            "GROUPBY" => {
                let n = count(i + 1)?;
                let keys: Vec<String> = args.iter().skip(i + 2).take(n).map(|s| prop(s)).collect();
                i += 2 + n;
                let mut reducers = Vec::new();
                while args
                    .get(i)
                    .is_some_and(|s| s.eq_ignore_ascii_case("REDUCE"))
                {
                    let name = args
                        .get(i + 1)
                        .ok_or("REDUCE needs a function")?
                        .to_ascii_uppercase();
                    let func = match name.as_str() {
                        "COUNT" => Func::Count,
                        "COUNT_DISTINCT" | "COUNT_DISTINCTISH" => Func::CountDistinct,
                        "SUM" => Func::Sum,
                        "AVG" => Func::Avg,
                        "MIN" => Func::Min,
                        "MAX" => Func::Max,
                        "TOLIST" => Func::ToList,
                        other => return Err(format!("unsupported reducer {other}")),
                    };
                    let nargs = count(i + 2)?;
                    let arg = (nargs > 0)
                        .then(|| args.get(i + 3).map(|s| prop(s)))
                        .flatten();
                    i += 3 + nargs;
                    let mut alias = format!(
                        "__generated_alias{}{}",
                        name.to_lowercase(),
                        arg.as_deref().unwrap_or("")
                    );
                    if args.get(i).is_some_and(|s| s.eq_ignore_ascii_case("AS")) {
                        alias = args.get(i + 1).ok_or("AS needs a name")?.to_string();
                        i += 2;
                    }
                    if func != Func::Count && arg.is_none() {
                        return Err(format!("{name} needs a property"));
                    }
                    reducers.push(Reducer { func, arg, alias });
                }
                plan.group = Some((keys, reducers));
            }
            "SORTBY" => {
                let n = count(i + 1)?;
                let mut j = i + 2;
                while j < i + 2 + n {
                    let field = prop(args.get(j).ok_or("bad SORTBY")?);
                    let desc = args
                        .get(j + 1)
                        .is_some_and(|s| s.eq_ignore_ascii_case("DESC"));
                    let has_dir = args.get(j + 1).is_some_and(|s| {
                        s.eq_ignore_ascii_case("DESC") || s.eq_ignore_ascii_case("ASC")
                    });
                    plan.sort.push((field, desc));
                    j += if has_dir { 2 } else { 1 };
                }
                i = j;
                if args.get(i).is_some_and(|s| s.eq_ignore_ascii_case("MAX")) {
                    plan.sort_max = Some(count(i + 1)?);
                    i += 2;
                }
            }
            "LIMIT" => {
                plan.limit = Some((count(i + 1)?, count(i + 2)?));
                i += 3;
            }
            "WITHCURSOR" => {
                plan.cursor = Some(10);
                i += 1;
            }
            "COUNT" if plan.cursor.is_some() => {
                plan.cursor = Some(count(i + 1)?.max(1));
                i += 2;
            }
            "APPLY" | "FILTER" => {
                return Err(format!("{} is not supported", args[i].to_ascii_uppercase()));
            }
            "MAXIDLE" | "DIALECT" | "TIMEOUT" => i += 2,
            "PARAMS" => i += 2 + count(i + 1)?,
            _ => i += 1,
        }
    }
    Ok(plan)
}

fn for_each_match(
    store: &Store,
    def: &std::sync::Arc<IndexDef>,
    cond: Option<&crate::search::query::Cond>,
    mut f: impl FnMut(&str, &StoreValue),
) {
    let resolved = cond.and_then(|c| {
        let fi = store.search_state().filter_index(store, def)?;
        let ids = match fi.resolve(def, c)? {
            Resolved::All => return Some(None),
            Resolved::Ids(ids) => ids,
        };
        Some(Some(
            ids.into_iter()
                .filter_map(|d| fi.key(d).map(str::to_string))
                .collect::<Vec<_>>(),
        ))
    });
    match resolved {
        Some(Some(keys)) => {
            for key in keys {
                if let Some(val) = store.data.get_ref(&key)
                    && !val.is_expired()
                {
                    f(&key, &val);
                }
            }
        }
        _ => store.data.for_each(|key, val| {
            if val.is_expired() || !def.matches_key(key) || !doc_storage_matches(val, def) {
                return;
            }
            if let Some(c) = cond
                && !cond_matches(c, val, def)
            {
                return;
            }
            f(key, val);
        }),
    }
}

fn cmp_values(a: Option<&Vec<u8>>, b: Option<&Vec<u8>>) -> std::cmp::Ordering {
    let num = |v: &Vec<u8>| {
        std::str::from_utf8(v)
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
    };
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, _) => std::cmp::Ordering::Greater,
        (_, None) => std::cmp::Ordering::Less,
        (Some(x), Some(y)) => match (num(x), num(y)) {
            (Some(p), Some(q)) => p.total_cmp(&q),
            _ => x.cmp(y),
        },
    }
}

pub(super) fn ft_aggregate(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    if parts.len() < 3 {
        return resp::write_store_err(out, "ERR wrong number of arguments for 'ft.aggregate'");
    }
    let Some(def) = store.search_state().def_arc(parts[1]) else {
        return resp::write_store_err(out, "ERR Unknown index name");
    };
    let plan = match parse_plan(&parts[3..]) {
        Ok(p) => p,
        Err(e) => return resp::write_store_err(out, &format!("ERR {e}")),
    };
    let Ok(pq) = parse_query(&def, parts[2]) else {
        return resp::write_store_err(out, "ERR syntax error");
    };

    let mut rows: Vec<Row> = Vec::new();
    let mut lists: ListMap = HashMap::new();
    if let Some((keys, reducers)) = &plan.group {
        let mut fields: Vec<String> = keys.clone();
        fields.extend(reducers.iter().filter_map(|r| r.arg.clone()));
        let opts = SearchOpts {
            return_fields: fields,
            return_all: false,
            ..SearchOpts::default()
        };
        let mut groups: HashMap<Vec<Option<Vec<u8>>>, Vec<Acc>> = HashMap::new();
        for_each_match(store, &def, pq.filter.as_ref(), |_, val| {
            let doc: HashMap<String, Vec<u8>> = doc_fields(val, &def, &opts).into_iter().collect();
            let gk: Vec<Option<Vec<u8>>> = keys.iter().map(|k| doc.get(k).cloned()).collect();
            let accs = groups
                .entry(gk)
                .or_insert_with(|| reducers.iter().map(Reducer::init).collect());
            for (acc, r) in accs.iter_mut().zip(reducers) {
                acc.add(
                    r.arg
                        .as_ref()
                        .and_then(|a| doc.get(a))
                        .map(|v| v.as_slice()),
                );
            }
        });
        for (gk, accs) in groups {
            let mut row: Row = keys.iter().cloned().zip(gk).collect();
            for (acc, r) in accs.iter().zip(reducers) {
                match acc.finish() {
                    Value::Nil => row.push((r.alias.clone(), None)),
                    Value::Bulk(b) => row.push((r.alias.clone(), Some(b))),
                    Value::List(items) => {
                        lists
                            .entry(rows.len())
                            .or_default()
                            .push((r.alias.clone(), items));
                        row.push((r.alias.clone(), Some(Vec::new())));
                    }
                }
            }
            rows.push(row);
        }
    } else {
        let opts = SearchOpts {
            return_fields: plan.load.clone(),
            return_all: plan.load_all,
            ..SearchOpts::default()
        };
        for_each_match(store, &def, pq.filter.as_ref(), |key, val| {
            let mut row: Row = doc_fields(val, &def, &opts)
                .into_iter()
                .map(|(k, v)| (k, Some(v)))
                .collect();
            row.push(("__key".to_string(), Some(key.as_bytes().to_vec())));
            rows.push(row);
        });
    }

    let mut order: Vec<usize> = (0..rows.len()).collect();
    if !plan.sort.is_empty() {
        let get = |r: &Row, f: &str| r.iter().find(|(k, _)| k == f).and_then(|(_, v)| v.clone());
        order.sort_by(|&a, &b| {
            for (f, desc) in &plan.sort {
                let o = cmp_values(get(&rows[a], f).as_ref(), get(&rows[b], f).as_ref());
                let o = if *desc { o.reverse() } else { o };
                if o != std::cmp::Ordering::Equal {
                    return o;
                }
            }
            std::cmp::Ordering::Equal
        });
        if let Some(m) = plan.sort_max {
            order.truncate(m);
        }
    }
    if let Some((off, num)) = plan.limit {
        order = order.into_iter().skip(off).take(num).collect();
    }
    let total = order.len();
    let rendered: Vec<Vec<u8>> = order
        .iter()
        .map(|&i| render_row(&rows[i], lists.get(&i).map(Vec::as_slice)))
        .collect();

    match plan.cursor {
        Some(count) => {
            let id = store.search_state().cursor_create();
            let page: Vec<Vec<u8>> = rendered.iter().take(count).cloned().collect();
            let rest: Vec<Vec<(String, Vec<u8>)>> = rendered[page.len()..]
                .iter()
                .map(|r| vec![(String::new(), r.clone())])
                .collect();
            let next = if rest.is_empty() {
                0
            } else {
                store.search_state().cursor_put(id, rest);
                id
            };
            resp::write_array_header(out, 2);
            write_rendered(total, &page, out);
            resp::write_integer(out, next as i64);
        }
        None => write_rendered(total, &rendered, out),
    }
}

fn render_row(row: &Row, lists: ListSlice<'_>) -> Vec<u8> {
    let mut out = Vec::new();
    resp::write_array_header(&mut out, row.len() * 2);
    for (k, v) in row {
        resp::write_bulk_bytes(&mut out, k.as_bytes());
        if let Some((_, items)) = lists.and_then(|l| l.iter().find(|(a, _)| a == k)) {
            resp::write_array_header(&mut out, items.len());
            for it in items {
                resp::write_bulk_bytes(&mut out, it);
            }
            continue;
        }
        match v {
            Some(b) => resp::write_bulk_bytes(&mut out, b),
            None => resp::write_nil(&mut out),
        }
    }
    out
}

fn write_rendered(total: usize, rows: &[Vec<u8>], out: &mut Vec<u8>) {
    resp::write_array_header(out, 1 + rows.len());
    resp::write_integer(out, total as i64);
    for r in rows {
        out.extend_from_slice(r);
    }
}

pub(super) fn ft_cursor(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let Some(action) = parts.get(1) else {
        return resp::write_store_err(out, "ERR wrong number of arguments");
    };
    let Some(name) = parts.get(2) else {
        return resp::write_store_err(out, "ERR wrong number of arguments");
    };
    let Some(cid) = parts.get(3).and_then(|v| v.parse::<u64>().ok()) else {
        return resp::write_store_err(out, "ERR bad cursor id");
    };
    match action.to_ascii_uppercase().as_str() {
        "READ" => {
            if !store.search_state().exists(name) {
                return resp::write_store_err(out, "ERR Unknown index name");
            }
            let count = parts
                .iter()
                .position(|p| p.eq_ignore_ascii_case("COUNT"))
                .and_then(|i| parts.get(i + 1))
                .and_then(|v| v.parse().ok())
                .unwrap_or(10);
            match store.search_state().cursor_read(cid, count) {
                Some((page, next)) => {
                    let rendered: Vec<Vec<u8>> = page
                        .into_iter()
                        .filter_map(|r| r.into_iter().next().map(|(_, b)| b))
                        .collect();
                    resp::write_array_header(out, 2);
                    write_rendered(rendered.len(), &rendered, out);
                    resp::write_integer(out, next as i64);
                }
                None => resp::write_store_err(out, "ERR Unknown cursor"),
            }
        }
        "DEL" => {
            if store.search_state().cursor_del(cid) {
                resp::write_ok(out)
            } else {
                resp::write_store_err(out, "ERR Unknown cursor")
            }
        }
        _ => resp::write_store_err(out, "ERR unknown cursor action"),
    }
}
