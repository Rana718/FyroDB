use crate::storage::store::Store;
use crate::storage::stream::{
   ClaimOpts, Claimed, ERR_INVALID_ID, StreamEntry, StreamId, Trim, TrimBy, parse_range_end,
   parse_range_start,
};
use crate::utils::resp;
use crate::{parse_int, wt};

macro_rules! st {
   ($out:expr, $e:expr) => {
      match $e {
         Ok(v) => v,
         Err(e) => return resp::write_store_err($out, e),
      }
   };
}

fn parse_trim(args: &[&str], i: &mut usize) -> Result<Option<Trim>, &'static str> {
   let kind = args[*i];
   let by_len = kind.eq_ignore_ascii_case("MAXLEN");
   if !by_len && !kind.eq_ignore_ascii_case("MINID") {
      return Ok(None);
   }
   *i += 1;
   let mut approx = false;
   if *i < args.len() && (args[*i] == "~" || args[*i] == "=") {
      approx = args[*i] == "~";
      *i += 1;
   }
   let v = *args.get(*i).ok_or("ERR syntax error")?;
   *i += 1;
   let by = if by_len {
      TrimBy::MaxLen(
         v.parse()
            .map_err(|_| "ERR value is not an integer or out of range")?,
      )
   } else {
      TrimBy::MinId(StreamId::parse(v).ok_or(ERR_INVALID_ID)?)
   };
   let mut limit = 0;
   if *i + 1 < args.len() && args[*i].eq_ignore_ascii_case("LIMIT") {
      if !approx {
         return Err("ERR syntax error, LIMIT cannot be used without the special ~ option");
      }
      limit = args[*i + 1]
         .parse()
         .map_err(|_| "ERR value is not an integer or out of range")?;
      *i += 2;
   }
   Ok(Some(Trim { by, approx, limit }))
}

pub fn xadd(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let [_, key, rest @ ..] = parts else {
      return resp::write_wrong_args(out, "xadd");
   };
   let mut nomkstream = false;
   let mut trim = None;
   let mut i = 0;
   while i < rest.len() {
      if rest[i].eq_ignore_ascii_case("NOMKSTREAM") {
         nomkstream = true;
         i += 1;
         continue;
      }
      match parse_trim(rest, &mut i) {
         Ok(Some(t)) => trim = Some(t),
         Ok(None) => break,
         Err(e) => return resp::write_store_err(out, e),
      }
   }
   if i >= rest.len() {
      return resp::write_wrong_args(out, "xadd");
   }
   let id_str = rest[i];
   let field_parts = &rest[i + 1..];
   if field_parts.is_empty() || field_parts.len() % 2 != 0 {
      return resp::write_wrong_args(out, "xadd");
   }
   const STACK_FIELDS: usize = 16;
   let n_pairs = field_parts.len() / 2;
   if n_pairs <= STACK_FIELDS {
      let mut stack_fields = [("", ""); STACK_FIELDS];
      for (idx, c) in field_parts.as_chunks::<2>().0.iter().enumerate() {
         stack_fields[idx] = (c[0], c[1]);
      }
      match store.xadd(key, id_str, &stack_fields[..n_pairs], trim, nomkstream) {
         Ok(Some(id)) => resp::write_bulk(out, &id),
         Ok(None) => resp::write_nil(out),
         Err(e) => resp::write_store_err(out, e),
      }
   } else {
      let fields: Vec<(&str, &str)> = field_parts
         .as_chunks::<2>()
         .0
         .iter()
         .map(|c| (c[0], c[1]))
         .collect();
      match store.xadd(key, id_str, &fields, trim, nomkstream) {
         Ok(Some(id)) => resp::write_bulk(out, &id),
         Ok(None) => resp::write_nil(out),
         Err(e) => resp::write_store_err(out, e),
      }
   }
}

pub fn xlen(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   match parts {
      [_, key] => resp::write_integer(out, wt!(out, store.xlen(key)) as i64),
      _ => resp::write_wrong_args(out, "xlen"),
   }
}

fn parse_count(out: &mut Vec<u8>, rest: &[&str]) -> Option<usize> {
   let mut count = 0usize;
   let mut i = 0;
   while i < rest.len() {
      if rest[i].eq_ignore_ascii_case("COUNT") && i + 1 < rest.len() {
         match rest[i + 1].parse::<i64>() {
            Ok(n) => count = n.max(0) as usize,
            Err(_) => {
               resp::write_err(out, "value is not an integer or out of range");
               return None;
            }
         }
         i += 2;
      } else {
         resp::write_err(out, "syntax error");
         return None;
      }
   }
   Some(count)
}

pub fn xrange(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let [_, key, start, end, rest @ ..] = parts else {
      return resp::write_wrong_args(out, "xrange");
   };
   let Some(count) = parse_count(out, rest) else {
      return;
   };
   if rest.len() >= 2 && count == 0 {
      return resp::write_array_header(out, 0);
   }
   let items = st!(out, store.xrange(key, start, end, count));
   write_stream_entries(out, &items);
}

pub fn xrevrange(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let [_, key, end, start, rest @ ..] = parts else {
      return resp::write_wrong_args(out, "xrevrange");
   };
   let Some(count) = parse_count(out, rest) else {
      return;
   };
   if rest.len() >= 2 && count == 0 {
      return resp::write_array_header(out, 0);
   }
   let items = st!(out, store.xrevrange(key, end, start, count));
   write_stream_entries(out, &items);
}

pub fn xtrim(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let [_, key, rest @ ..] = parts else {
      return resp::write_wrong_args(out, "xtrim");
   };
   if rest.is_empty() {
      return resp::write_wrong_args(out, "xtrim");
   }
   let mut i = 0;
   let trim = match parse_trim(rest, &mut i) {
      Ok(Some(t)) if i == rest.len() => t,
      Ok(_) => return resp::write_err(out, "syntax error"),
      Err(e) => return resp::write_store_err(out, e),
   };
   resp::write_integer(out, st!(out, store.xtrim(key, trim)) as i64);
}

pub fn xdel(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   match parts {
      [_, key, ids @ ..] if !ids.is_empty() => {
         resp::write_integer(out, st!(out, store.xdel(key, ids)) as i64);
      }
      _ => resp::write_wrong_args(out, "xdel"),
   }
}

pub fn xgroup(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let [_, sub, rest @ ..] = parts else {
      return resp::write_wrong_args(out, "xgroup");
   };
   let sub = sub.to_ascii_uppercase();
   match (sub.as_str(), rest) {
      ("CREATE", [key, group, id, opts @ ..]) => {
         let mut mkstream = false;
         let mut i = 0;
         while i < opts.len() {
            if opts[i].eq_ignore_ascii_case("MKSTREAM") {
               mkstream = true;
               i += 1;
            } else if opts[i].eq_ignore_ascii_case("ENTRIESREAD") && i + 1 < opts.len() {
               i += 2;
            } else {
               return resp::write_err(out, "syntax error");
            }
         }
         st!(out, store.xgroup_create(key, group, id, mkstream));
         resp::write_ok(out);
      }
      ("SETID", [key, group, id, ..]) => {
         st!(out, store.xgroup_setid(key, group, id));
         resp::write_ok(out);
      }
      ("DESTROY", [key, group]) => {
         resp::write_integer(out, st!(out, store.xgroup_destroy(key, group)) as i64);
      }
      ("CREATECONSUMER", [key, group, consumer]) => {
         resp::write_integer(
            out,
            st!(out, store.xgroup_createconsumer(key, group, consumer)) as i64,
         );
      }
      ("DELCONSUMER", [key, group, consumer]) => {
         resp::write_integer(
            out,
            st!(out, store.xgroup_delconsumer(key, group, consumer)) as i64,
         );
      }
      ("HELP", _) => {
         let lines = [
            "XGROUP <subcommand> [<arg> [value] [opt] ...]",
            "CREATE <key> <groupname> <id|$> [MKSTREAM] [ENTRIESREAD <n>]",
            "CREATECONSUMER <key> <groupname> <consumer>",
            "DELCONSUMER <key> <groupname> <consumer>",
            "DESTROY <key> <groupname>",
            "SETID <key> <groupname> <id|$>",
         ];
         resp::write_array_header(out, lines.len());
         for l in lines {
            resp::write_simple(out, l);
         }
      }
      ("CREATE" | "SETID" | "DESTROY" | "CREATECONSUMER" | "DELCONSUMER", _) => {
         resp::write_wrong_args(out, &format!("xgroup|{}", sub.to_ascii_lowercase()))
      }
      _ => resp::write_err(out, "unknown XGROUP subcommand"),
   }
}

pub fn xack(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let [_, key, group, ids @ ..] = parts else {
      return resp::write_wrong_args(out, "xack");
   };
   if ids.is_empty() {
      return resp::write_wrong_args(out, "xack");
   }
   resp::write_integer(out, st!(out, store.xack(key, group, ids)) as i64);
}

pub fn xpending(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let [_, key, group, rest @ ..] = parts else {
      return resp::write_wrong_args(out, "xpending");
   };
   if rest.is_empty() {
      let (n, bounds, per) = st!(out, store.xpending_summary(key, group));
      resp::write_array_header(out, 4);
      resp::write_integer(out, n as i64);
      match bounds {
         Some((a, b)) => {
            resp::write_bulk(out, &a.to_string());
            resp::write_bulk(out, &b.to_string());
            resp::write_array_header(out, per.len());
            for (c, k) in per {
               resp::write_array_header(out, 2);
               resp::write_bulk(out, &c);
               resp::write_bulk(out, &k.to_string());
            }
         }
         None => {
            resp::write_nil(out);
            resp::write_nil(out);
            out.extend_from_slice(b"*-1\r\n");
         }
      }
      return;
   }
   let mut i = 0;
   let mut min_idle = 0u64;
   if rest[0].eq_ignore_ascii_case("IDLE") {
      let Some(v) = rest.get(1) else {
         return resp::write_err(out, "syntax error");
      };
      min_idle = parse_int!(out, v, u64);
      i = 2;
   }
   let (Some(start), Some(end), Some(count)) = (rest.get(i), rest.get(i + 1), rest.get(i + 2))
   else {
      return resp::write_err(out, "syntax error");
   };
   let consumer = rest.get(i + 3).copied();
   if rest.len() > i + 4 {
      return resp::write_err(out, "syntax error");
   }
   let count: i64 = parse_int!(out, count, i64);
   let (Some(a), Some(b)) = (
      st!(out, parse_range_start(start)),
      st!(out, parse_range_end(end)),
   ) else {
      return resp::write_array_header(out, 0);
   };
   let rows = st!(
      out,
      store.xpending_range(key, group, min_idle, a, b, count.max(0) as usize, consumer)
   );
   resp::write_array_header(out, rows.len());
   for p in rows {
      resp::write_array_header(out, 4);
      resp::write_bulk(out, &p.id.to_string());
      resp::write_bulk(out, &p.consumer);
      resp::write_integer(out, p.idle as i64);
      resp::write_integer(out, p.deliveries as i64);
   }
}

fn write_claimed(out: &mut Vec<u8>, items: &Claimed, justid: bool) {
   resp::write_array_header(out, items.len());
   for (id, fields) in items {
      if justid {
         resp::write_bulk(out, &id.to_string());
         continue;
      }
      resp::write_array_header(out, 2);
      resp::write_bulk(out, &id.to_string());
      match fields {
         Some(f) => write_fields(out, f),
         None => out.extend_from_slice(b"*-1\r\n"),
      }
   }
}

pub fn xclaim(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let [_, key, group, consumer, min_idle, rest @ ..] = parts else {
      return resp::write_wrong_args(out, "xclaim");
   };
   let min_idle = parse_int!(out, min_idle, i64).max(0) as u64;
   let mut ids = Vec::new();
   let mut i = 0;
   while i < rest.len() {
      match StreamId::parse(rest[i]) {
         Some(id) => ids.push(id),
         None => break,
      }
      i += 1;
   }
   if ids.is_empty() {
      return resp::write_wrong_args(out, "xclaim");
   }
   let mut o = ClaimOpts {
      idle: None,
      time: None,
      retry_count: None,
      force: false,
      justid: false,
      last_id: None,
   };
   while i < rest.len() {
      let opt = rest[i].to_ascii_uppercase();
      let val = rest.get(i + 1);
      match (opt.as_str(), val) {
         ("FORCE", _) => {
            o.force = true;
            i += 1;
         }
         ("JUSTID", _) => {
            o.justid = true;
            i += 1;
         }
         ("IDLE", Some(v)) => {
            o.idle = Some(parse_int!(out, v, i64).max(0) as u64);
            i += 2;
         }
         ("TIME", Some(v)) => {
            o.time = Some(parse_int!(out, v, i64).max(0) as u64);
            i += 2;
         }
         ("RETRYCOUNT", Some(v)) => {
            o.retry_count = Some(parse_int!(out, v, i64).max(0) as u64);
            i += 2;
         }
         ("LASTID", Some(v)) => {
            o.last_id = Some(st!(out, StreamId::parse(v).ok_or(ERR_INVALID_ID)));
            i += 2;
         }
         _ => return resp::write_err(out, &format!("Unrecognized XCLAIM option '{}'", rest[i])),
      }
   }
   let items = st!(out, store.xclaim(key, group, consumer, min_idle, &ids, &o));
   write_claimed(out, &items, o.justid);
}

pub fn xautoclaim(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let [_, key, group, consumer, min_idle, start, rest @ ..] = parts else {
      return resp::write_wrong_args(out, "xautoclaim");
   };
   let min_idle = parse_int!(out, min_idle, i64).max(0) as u64;
   let start = match st!(out, parse_range_start(start)) {
      Some(s) => s,
      None => StreamId::MAX,
   };
   let mut count = 100usize;
   let mut justid = false;
   let mut i = 0;
   while i < rest.len() {
      if rest[i].eq_ignore_ascii_case("COUNT") && i + 1 < rest.len() {
         let n = parse_int!(out, rest[i + 1], i64);
         if n < 1 {
            return resp::write_err(out, "COUNT must be > 0");
         }
         count = n as usize;
         i += 2;
      } else if rest[i].eq_ignore_ascii_case("JUSTID") {
         justid = true;
         i += 1;
      } else {
         return resp::write_err(out, "syntax error");
      }
   }
   let (next, items, deleted) = st!(
      out,
      store.xautoclaim(key, group, consumer, min_idle, start, count, justid)
   );
   resp::write_array_header(out, 3);
   resp::write_bulk(out, &next.to_string());
   write_claimed(out, &items, justid);
   resp::write_array_header(out, deleted.len());
   for id in deleted {
      resp::write_bulk(out, &id.to_string());
   }
}

fn write_entry_or_nil(out: &mut Vec<u8>, e: &Option<StreamEntry>) {
   match e {
      Some((id, f)) => {
         resp::write_array_header(out, 2);
         resp::write_bulk(out, id);
         write_fields(out, f);
      }
      None => resp::write_nil(out),
   }
}

pub fn xinfo(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   let [_, sub, rest @ ..] = parts else {
      return resp::write_wrong_args(out, "xinfo");
   };
   if sub.eq_ignore_ascii_case("STREAM") {
      let [key, ..] = rest else {
         return resp::write_wrong_args(out, "xinfo|stream");
      };
      let Some(i) = st!(out, store.xinfo_stream(key)) else {
         return resp::write_err(out, "no such key");
      };
      resp::write_array_header(out, 20);
      resp::write_bulk(out, "length");
      resp::write_integer(out, i.length as i64);
      resp::write_bulk(out, "radix-tree-keys");
      resp::write_integer(out, i.nodes as i64);
      resp::write_bulk(out, "radix-tree-nodes");
      resp::write_integer(out, i.nodes as i64);
      resp::write_bulk(out, "last-generated-id");
      resp::write_bulk(out, &i.last_id.to_string());
      resp::write_bulk(out, "max-deleted-entry-id");
      resp::write_bulk(out, &i.max_deleted.to_string());
      resp::write_bulk(out, "entries-added");
      resp::write_integer(out, i.entries_added as i64);
      resp::write_bulk(out, "recorded-first-entry-id");
      resp::write_bulk(
         out,
         &i.first.as_ref().map_or("0-0".to_string(), |e| e.0.clone()),
      );
      resp::write_bulk(out, "groups");
      resp::write_integer(out, i.groups as i64);
      resp::write_bulk(out, "first-entry");
      write_entry_or_nil(out, &i.first);
      resp::write_bulk(out, "last-entry");
      write_entry_or_nil(out, &i.last);
   } else if sub.eq_ignore_ascii_case("GROUPS") {
      let [key] = rest else {
         return resp::write_wrong_args(out, "xinfo|groups");
      };
      let Some(groups) = st!(out, store.xinfo_groups(key)) else {
         return resp::write_err(out, "no such key");
      };
      resp::write_array_header(out, groups.len());
      for g in groups {
         resp::write_array_header(out, 12);
         resp::write_bulk(out, "name");
         resp::write_bulk(out, &g.name);
         resp::write_bulk(out, "consumers");
         resp::write_integer(out, g.consumers as i64);
         resp::write_bulk(out, "pending");
         resp::write_integer(out, g.pending as i64);
         resp::write_bulk(out, "last-delivered-id");
         resp::write_bulk(out, &g.last_delivered.to_string());
         resp::write_bulk(out, "entries-read");
         match g.entries_read {
            Some(v) => resp::write_integer(out, v as i64),
            None => resp::write_nil(out),
         }
         resp::write_bulk(out, "lag");
         match g.lag {
            Some(v) => resp::write_integer(out, v as i64),
            None => resp::write_nil(out),
         }
      }
   } else if sub.eq_ignore_ascii_case("CONSUMERS") {
      let [key, group] = rest else {
         return resp::write_wrong_args(out, "xinfo|consumers");
      };
      let cs = st!(out, store.xinfo_consumers(key, group));
      resp::write_array_header(out, cs.len());
      for c in cs {
         resp::write_array_header(out, 8);
         resp::write_bulk(out, "name");
         resp::write_bulk(out, &c.name);
         resp::write_bulk(out, "pending");
         resp::write_integer(out, c.pending as i64);
         resp::write_bulk(out, "idle");
         resp::write_integer(out, c.idle as i64);
         resp::write_bulk(out, "inactive");
         resp::write_integer(out, c.inactive);
      }
   } else {
      resp::write_err(out, "unknown XINFO subcommand");
   }
}

pub struct XReadReq {
   pub count: usize,
   pub block: Option<u64>,
   pub group: Option<(String, String)>,
   pub noack: bool,
   pub keys: Vec<String>,
   pub ids: Vec<String>,
}

pub fn parse_xread(parts: &[&str], grouped: bool) -> Result<XReadReq, String> {
   let name = if grouped { "xreadgroup" } else { "xread" };
   let mut req = XReadReq {
      count: 0,
      block: None,
      group: None,
      noack: false,
      keys: Vec::new(),
      ids: Vec::new(),
   };
   let mut i = 1;
   let mut streams = 0;
   while i < parts.len() {
      let a = parts[i];
      if a.eq_ignore_ascii_case("COUNT") && i + 1 < parts.len() {
         let n: i64 = parts[i + 1]
            .parse()
            .map_err(|_| "ERR value is not an integer or out of range".to_string())?;
         req.count = n.max(0) as usize;
         i += 2;
      } else if a.eq_ignore_ascii_case("BLOCK") && i + 1 < parts.len() {
         let n: i64 = parts[i + 1]
            .parse()
            .map_err(|_| "ERR timeout is not an integer or out of range".to_string())?;
         if n < 0 {
            return Err("ERR timeout is negative".into());
         }
         req.block = Some(n as u64);
         i += 2;
      } else if grouped && a.eq_ignore_ascii_case("GROUP") && i + 2 < parts.len() {
         req.group = Some((parts[i + 1].to_string(), parts[i + 2].to_string()));
         i += 3;
      } else if grouped && a.eq_ignore_ascii_case("NOACK") {
         req.noack = true;
         i += 1;
      } else if a.eq_ignore_ascii_case("STREAMS") {
         streams = i + 1;
         break;
      } else {
         return Err("ERR syntax error".into());
      }
   }
   if grouped && req.group.is_none() {
      return Err("ERR Missing GROUP option for XREADGROUP".into());
   }
   let args = parts.get(streams..).unwrap_or(&[]);
   if streams == 0 || args.is_empty() {
      return Err(format!(
         "ERR wrong number of arguments for '{name}' command"
      ));
   }
   if args.len() % 2 != 0 {
      return Err("ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.".into());
   }
   let half = args.len() / 2;
   req.keys = args[..half].iter().map(|s| s.to_string()).collect();
   req.ids = args[half..].iter().map(|s| s.to_string()).collect();
   for id in &req.ids {
      let ok = match (grouped, id.as_str()) {
         (true, ">") | (false, "$") | (false, "+") => true,
         (true, "$") => false,
         _ => StreamId::parse(id).is_some(),
      };
      if !ok {
         return Err(if grouped && id == "$" {
            "ERR The $ ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The $ ID would just return an empty result set.".into()
         } else {
            ERR_INVALID_ID.into()
         });
      }
   }
   Ok(req)
}

impl XReadReq {
   pub fn resolve_dollar(&mut self, store: &Store) {
      if self.group.is_some() {
         return;
      }
      for (k, id) in self.keys.iter().zip(self.ids.iter_mut()) {
         if id == "$" {
            *id = store.xlast_id(k).unwrap_or(StreamId::MIN).to_string();
         }
      }
   }

   pub fn serve(&self, store: &Store, out: &mut Vec<u8>, allow_empty: bool) -> bool {
      match &self.group {
         None => self.serve_plain(store, out, allow_empty),
         Some((g, c)) => self.serve_group(store, g, c, out, allow_empty),
      }
   }

   fn serve_plain(&self, store: &Store, out: &mut Vec<u8>, allow_empty: bool) -> bool {
      let mut results: Vec<(&str, Vec<StreamEntry>)> = Vec::new();
      for (k, id) in self.keys.iter().zip(&self.ids) {
         let entries = match id.as_str() {
            "$" => Vec::new(),
            "+" => match store.xrevrange(k, "+", "-", 1) {
               Ok(v) => v,
               Err(e) => {
                  resp::write_store_err(out, e);
                  return true;
               }
            },
            _ => {
               let after = StreamId::parse(id).unwrap_or(StreamId::MIN);
               match store.xread_after(k, after, self.count) {
                  Ok(v) => v,
                  Err(e) => {
                     resp::write_store_err(out, e);
                     return true;
                  }
               }
            }
         };
         if !entries.is_empty() {
            results.push((k, entries));
         }
      }
      if results.is_empty() {
         if allow_empty {
            out.extend_from_slice(b"*-1\r\n");
            return true;
         }
         return false;
      }
      resp::write_array_header(out, results.len());
      for (key, entries) in &results {
         resp::write_array_header(out, 2);
         resp::write_bulk(out, key);
         write_stream_entries(out, entries);
      }
      true
   }

   fn serve_group(
      &self,
      store: &Store,
      g: &str,
      c: &str,
      out: &mut Vec<u8>,
      allow_empty: bool,
   ) -> bool {
      let mut results: Vec<(&str, Claimed)> = Vec::new();
      let mut any = false;
      for (k, id) in self.keys.iter().zip(&self.ids) {
         let after = if id == ">" { None } else { StreamId::parse(id) };
         let items = match store.xreadgroup(k, g, c, after, self.count, self.noack) {
            Ok(v) => v,
            Err(e) => {
               if e == crate::storage::stream::ERR_NOGROUP {
                  let msg = format!(
                     "NOGROUP No such key '{k}' or consumer group '{g}' in XREADGROUP with GROUP option"
                  );
                  resp::write_store_err(out, &msg);
               } else {
                  resp::write_store_err(out, e);
               }
               return true;
            }
         };
         if after.is_some() || !items.is_empty() {
            any = true;
            results.push((k, items));
         }
      }
      if !any {
         if allow_empty {
            out.extend_from_slice(b"*-1\r\n");
            return true;
         }
         return false;
      }
      resp::write_array_header(out, results.len());
      for (key, items) in &results {
         resp::write_array_header(out, 2);
         resp::write_bulk(out, key);
         write_claimed(out, items, false);
      }
      true
   }
}

pub fn xread(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   match parse_xread(parts, false) {
      Ok(mut req) => {
         req.resolve_dollar(store);
         req.serve(store, out, true);
      }
      Err(e) => resp::write_store_err(out, &e),
   }
}

pub fn xreadgroup(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
   match parse_xread(parts, true) {
      Ok(req) => {
         req.serve(store, out, true);
      }
      Err(e) => resp::write_store_err(out, &e),
   }
}

fn write_fields(out: &mut Vec<u8>, fields: &[(String, String)]) {
   resp::write_array_header(out, fields.len() * 2);
   for (f, v) in fields {
      resp::write_bulk(out, f);
      resp::write_bulk(out, v);
   }
}

fn write_stream_entries(out: &mut Vec<u8>, items: &[StreamEntry]) {
   resp::write_array_header(out, items.len());
   for (id, fields) in items {
      resp::write_array_header(out, 2);
      resp::write_bulk(out, id);
      write_fields(out, fields);
   }
}
