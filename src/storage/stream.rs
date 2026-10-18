use crate::storage::store::Store;
use crate::storage::value::{FyroDB, SmallStr, StoreValue, now_ms};
use foldhash::{HashMap, HashMapExt};
use std::collections::{BTreeMap, BTreeSet};

pub type Fields = Vec<(String, String)>;
pub type StreamEntry = (String, Fields);

const NODE_MAX_ENTRIES: u32 = 100;
const NODE_MAX_BYTES: usize = 4096;

pub const ERR_INVALID_ID: &str = "ERR Invalid stream ID specified as stream command argument";
pub const ERR_ID_TOO_SMALL: &str =
   "ERR The ID specified in XADD is equal or smaller than the target stream top item";
pub const ERR_ID_ZERO: &str = "ERR The ID specified in XADD must be greater than 0-0";
pub const ERR_NOGROUP: &str = "NOGROUP No such key or consumer group";
pub const ERR_BUSYGROUP: &str = "BUSYGROUP Consumer Group name already exists";
pub const ERR_NO_KEY_GROUP: &str = "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.";

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct StreamId {
   pub ms: u64,
   pub seq: u64,
}

impl std::fmt::Display for StreamId {
   fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      write!(f, "{}-{}", self.ms, self.seq)
   }
}

impl StreamId {
   pub const MIN: StreamId = StreamId { ms: 0, seq: 0 };
   pub const MAX: StreamId = StreamId {
      ms: u64::MAX,
      seq: u64::MAX,
   };

   pub fn new(ms: u64, seq: u64) -> Self {
      Self { ms, seq }
   }

   pub fn parse(s: &str) -> Option<Self> {
      Self::parse_with(s, 0)
   }

   pub fn parse_with(s: &str, missing_seq: u64) -> Option<Self> {
      match s.split_once('-') {
         Some((ms, seq)) => Some(Self {
            ms: ms.parse().ok()?,
            seq: seq.parse().ok()?,
         }),
         None => Some(Self {
            ms: s.parse().ok()?,
            seq: missing_seq,
         }),
      }
   }

   pub fn min() -> Self {
      Self::MIN
   }

   pub fn max() -> Self {
      Self::MAX
   }

   pub fn next(self) -> Option<Self> {
      if self.seq < u64::MAX {
         Some(Self::new(self.ms, self.seq + 1))
      } else if self.ms < u64::MAX {
         Some(Self::new(self.ms + 1, 0))
      } else {
         None
      }
   }

   pub fn prev(self) -> Option<Self> {
      if self.seq > 0 {
         Some(Self::new(self.ms, self.seq - 1))
      } else if self.ms > 0 {
         Some(Self::new(self.ms - 1, u64::MAX))
      } else {
         None
      }
   }
}

pub fn parse_range_start(s: &str) -> Result<Option<StreamId>, &'static str> {
   if s == "-" {
      return Ok(Some(StreamId::MIN));
   }
   if s == "+" {
      return Ok(Some(StreamId::MAX));
   }
   if let Some(rest) = s.strip_prefix('(') {
      let id = StreamId::parse_with(rest, 0).ok_or(ERR_INVALID_ID)?;
      return Ok(id.next());
   }
   StreamId::parse_with(s, 0).map(Some).ok_or(ERR_INVALID_ID)
}

pub fn parse_range_end(s: &str) -> Result<Option<StreamId>, &'static str> {
   if s == "+" {
      return Ok(Some(StreamId::MAX));
   }
   if s == "-" {
      return Ok(Some(StreamId::MIN));
   }
   if let Some(rest) = s.strip_prefix('(') {
      let id = StreamId::parse_with(rest, u64::MAX).ok_or(ERR_INVALID_ID)?;
      return Ok(id.prev());
   }
   StreamId::parse_with(s, u64::MAX)
      .map(Some)
      .ok_or(ERR_INVALID_ID)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum IdSpec {
   Auto,
   AutoSeq(u64),
   Exact(StreamId),
}

impl IdSpec {
   pub fn parse(s: &str) -> Option<Self> {
      if s == "*" {
         return Some(Self::Auto);
      }
      if let Some(ms) = s.strip_suffix("-*") {
         return ms.parse().ok().map(Self::AutoSeq);
      }
      StreamId::parse_with(s, 0).map(Self::Exact)
   }
}

#[derive(Clone, Copy, Debug)]
pub enum TrimBy {
   MaxLen(usize),
   MinId(StreamId),
}

#[derive(Clone, Copy, Debug)]
pub struct Trim {
   pub by: TrimBy,
   pub approx: bool,
   pub limit: usize,
}

#[inline]
fn put_varint(buf: &mut Vec<u8>, mut v: u64) {
   while v >= 0x80 {
      buf.push((v as u8) | 0x80);
      v >>= 7;
   }
   buf.push(v as u8);
}

#[inline]
fn get_varint(buf: &[u8], pos: &mut usize) -> u64 {
   let mut v = 0u64;
   let mut shift = 0;
   loop {
      let b = buf[*pos];
      *pos += 1;
      v |= ((b & 0x7f) as u64) << shift;
      if b < 0x80 {
         return v;
      }
      shift += 7;
   }
}

fn get_varint_checked(buf: &[u8], pos: &mut usize) -> Option<u64> {
   let mut v = 0u64;
   let mut shift = 0u32;
   loop {
      let b = *buf.get(*pos)?;
      *pos += 1;
      if shift > 63 {
         return None;
      }
      v |= ((b & 0x7f) as u64).checked_shl(shift)?;
      if b < 0x80 {
         return Some(v);
      }
      shift += 7;
   }
}

#[derive(Clone, Default)]
struct Node {
   buf: Vec<u8>,
   count: u32,
   live: u32,
}

#[derive(Clone, Copy)]
struct Raw<'a> {
   id: StreamId,
   flag: usize,
   deleted: bool,
   nfields: usize,
   body: &'a [u8],
}

impl<'a> Raw<'a> {
   fn fields(&self) -> FieldIter<'a> {
      FieldIter {
         buf: self.body,
         pos: 0,
         left: self.nfields,
      }
   }

   fn owned(&self) -> Fields {
      self
         .fields()
         .map(|(k, v)| (k.to_string(), v.to_string()))
         .collect()
   }
}

pub struct FieldIter<'a> {
   buf: &'a [u8],
   pos: usize,
   left: usize,
}

impl<'a> FieldIter<'a> {
   #[inline]
   fn take(&mut self) -> &'a str {
      let n = get_varint(self.buf, &mut self.pos) as usize;
      let s = &self.buf[self.pos..self.pos + n];
      self.pos += n;
      unsafe { std::str::from_utf8_unchecked(s) }
   }
}

impl<'a> Iterator for FieldIter<'a> {
   type Item = (&'a str, &'a str);

   #[inline]
   fn next(&mut self) -> Option<Self::Item> {
      if self.left == 0 {
         return None;
      }
      self.left -= 1;
      let k = self.take();
      let v = self.take();
      Some((k, v))
   }

   fn size_hint(&self) -> (usize, Option<usize>) {
      (self.left, Some(self.left))
   }
}

impl ExactSizeIterator for FieldIter<'_> {}

struct NodeIter<'a> {
   buf: &'a [u8],
   pos: usize,
   master: u64,
}

impl<'a> Iterator for NodeIter<'a> {
   type Item = Raw<'a>;

   #[inline]
   fn next(&mut self) -> Option<Raw<'a>> {
      if self.pos >= self.buf.len() {
         return None;
      }
      let flag = self.pos;
      let deleted = self.buf[flag] != 0;
      self.pos += 1;
      let ms = self.master + get_varint(self.buf, &mut self.pos);
      let seq = get_varint(self.buf, &mut self.pos);
      let nfields = get_varint(self.buf, &mut self.pos) as usize;
      let start = self.pos;
      for _ in 0..nfields * 2 {
         let n = get_varint(self.buf, &mut self.pos) as usize;
         self.pos += n;
      }
      Some(Raw {
         id: StreamId::new(ms, seq),
         flag,
         deleted,
         nfields,
         body: &self.buf[start..self.pos],
      })
   }
}

impl Node {
   #[inline]
   fn iter(&self, master: StreamId) -> NodeIter<'_> {
      NodeIter {
         buf: &self.buf,
         pos: 0,
         master: master.ms,
      }
   }

   fn push<K: AsRef<str>, V: AsRef<str>>(
      &mut self,
      master: StreamId,
      id: StreamId,
      fields: &[(K, V)],
   ) {
      self.buf.push(0);
      put_varint(&mut self.buf, id.ms - master.ms);
      put_varint(&mut self.buf, id.seq);
      put_varint(&mut self.buf, fields.len() as u64);
      for (k, v) in fields {
         let (k, v) = (k.as_ref().as_bytes(), v.as_ref().as_bytes());
         put_varint(&mut self.buf, k.len() as u64);
         self.buf.extend_from_slice(k);
         put_varint(&mut self.buf, v.len() as u64);
         self.buf.extend_from_slice(v);
      }
      self.count += 1;
      self.live += 1;
   }

   fn validate(buf: &[u8], master: StreamId, count: u32, live: u32) -> Option<StreamId> {
      let mut pos = 0;
      let (mut n, mut l) = (0u32, 0u32);
      let mut prev: Option<StreamId> = None;
      while pos < buf.len() {
         let flag = buf[pos];
         if flag > 1 {
            return None;
         }
         pos += 1;
         let ms = master.ms.checked_add(get_varint_checked(buf, &mut pos)?)?;
         let seq = get_varint_checked(buf, &mut pos)?;
         let id = StreamId::new(ms, seq);
         if id < master || prev.is_some_and(|p| id <= p) {
            return None;
         }
         prev = Some(id);
         let nf = get_varint_checked(buf, &mut pos)?;
         for _ in 0..nf.checked_mul(2)? {
            let len = get_varint_checked(buf, &mut pos)? as usize;
            let end = pos.checked_add(len)?;
            std::str::from_utf8(buf.get(pos..end)?).ok()?;
            pos = end;
         }
         n += 1;
         if flag == 0 {
            l += 1;
         }
      }
      (n == count && l == live && live > 0).then_some(prev?)
   }
}

#[derive(Clone)]
pub struct PelEntry {
   pub consumer: SmallStr,
   pub delivery_time: u64,
   pub delivery_count: u64,
}

#[derive(Clone, Default)]
pub struct ConsumerData {
   pub pel: BTreeSet<StreamId>,
   pub seen_time: u64,
   pub active_time: u64,
}

#[derive(Clone)]
pub struct ConsumerGroup {
   pub last_delivered: StreamId,
   pub entries_read: Option<u64>,
   pub pel: BTreeMap<StreamId, PelEntry>,
   pub consumers: HashMap<SmallStr, ConsumerData>,
}

impl ConsumerGroup {
   pub fn new(last_delivered: StreamId) -> Self {
      Self {
         last_delivered,
         entries_read: None,
         pel: BTreeMap::new(),
         consumers: HashMap::new(),
      }
   }

   pub fn consumer(&mut self, name: &str, now: u64) -> &mut ConsumerData {
      let c = self
         .consumers
         .entry(SmallStr::new(name))
         .or_insert_with(|| ConsumerData {
            pel: BTreeSet::new(),
            seen_time: now,
            active_time: 0,
         });
      c.seen_time = now;
      c
   }

   fn assign(&mut self, id: StreamId, consumer: &str, now: u64, bump: bool) {
      let owner = SmallStr::new(consumer);
      match self.pel.get_mut(&id) {
         Some(p) => {
            if p.consumer.as_str() != consumer {
               if let Some(prev) = self.consumers.get_mut(p.consumer.as_str()) {
                  prev.pel.remove(&id);
               }
               p.consumer = owner;
            }
            p.delivery_time = now;
            if bump {
               p.delivery_count += 1;
            }
         }
         None => {
            self.pel.insert(
               id,
               PelEntry {
                  consumer: owner,
                  delivery_time: now,
                  delivery_count: 1,
               },
            );
         }
      }
      self.consumer(consumer, now).pel.insert(id);
   }

   fn drop_pending(&mut self, id: StreamId) -> bool {
      match self.pel.remove(&id) {
         Some(p) => {
            if let Some(c) = self.consumers.get_mut(p.consumer.as_str()) {
               c.pel.remove(&id);
            }
            true
         }
         None => false,
      }
   }
}

#[derive(Clone, Default)]
pub struct StreamData {
   nodes: BTreeMap<StreamId, Node>,
   len: usize,
   pub last_id: StreamId,
   pub max_deleted: StreamId,
   pub entries_added: u64,
   pub groups: HashMap<SmallStr, ConsumerGroup>,
}

pub struct ClaimOpts {
   pub idle: Option<u64>,
   pub time: Option<u64>,
   pub retry_count: Option<u64>,
   pub force: bool,
   pub justid: bool,
   pub last_id: Option<StreamId>,
}

pub struct Pending {
   pub id: StreamId,
   pub consumer: String,
   pub idle: u64,
   pub deliveries: u64,
}

pub struct GroupInfo {
   pub name: String,
   pub consumers: usize,
   pub pending: usize,
   pub last_delivered: StreamId,
   pub entries_read: Option<u64>,
   pub lag: Option<u64>,
}

pub struct ConsumerInfo {
   pub name: String,
   pub pending: usize,
   pub idle: u64,
   pub inactive: i64,
}

pub struct StreamInfo {
   pub length: usize,
   pub nodes: usize,
   pub last_id: StreamId,
   pub max_deleted: StreamId,
   pub entries_added: u64,
   pub groups: usize,
   pub first: Option<StreamEntry>,
   pub last: Option<StreamEntry>,
}

pub type Claimed = Vec<(StreamId, Option<Fields>)>;
pub type PendingSummary = (usize, Option<(StreamId, StreamId)>, Vec<(String, usize)>);

impl StreamData {
   pub fn new() -> Self {
      Self::default()
   }

   #[inline]
   pub fn len(&self) -> usize {
      self.len
   }

   #[inline]
   pub fn is_empty(&self) -> bool {
      self.len == 0
   }

   pub fn node_count(&self) -> usize {
      self.nodes.len()
   }

   pub fn memory_bytes(&self) -> usize {
      let nodes: usize = self.nodes.values().map(|n| n.buf.capacity() + 64).sum();
      let groups: usize = self
         .groups
         .values()
         .map(|g| 64 + g.pel.len() * 80 + g.consumers.len() * 96)
         .sum();
      nodes + groups
   }

   pub fn resolve_id(&self, spec: IdSpec) -> Result<StreamId, &'static str> {
      let last = self.last_id;
      match spec {
         IdSpec::Auto => {
            let ms = now_ms();
            if ms > last.ms {
               Ok(StreamId::new(ms, 0))
            } else {
               last.next().ok_or(ERR_ID_TOO_SMALL)
            }
         }
         IdSpec::AutoSeq(ms) => {
            if ms > last.ms {
               Ok(StreamId::new(ms, 0))
            } else if ms == last.ms && last.seq < u64::MAX {
               Ok(StreamId::new(ms, last.seq + 1))
            } else {
               Err(ERR_ID_TOO_SMALL)
            }
         }
         IdSpec::Exact(id) => {
            if id == StreamId::MIN {
               Err(ERR_ID_ZERO)
            } else if id <= last {
               Err(ERR_ID_TOO_SMALL)
            } else {
               Ok(id)
            }
         }
      }
   }

   pub fn add<K: AsRef<str>, V: AsRef<str>>(
      &mut self,
      spec: IdSpec,
      fields: &[(K, V)],
   ) -> Result<StreamId, &'static str> {
      let id = self.resolve_id(spec)?;
      self.append(id, fields);
      Ok(id)
   }

   pub fn append<K: AsRef<str>, V: AsRef<str>>(&mut self, id: StreamId, fields: &[(K, V)]) {
      let fresh = match self.nodes.last_key_value() {
         None => true,
         Some((_, n)) => n.count >= NODE_MAX_ENTRIES || n.buf.len() >= NODE_MAX_BYTES,
      };
      if fresh {
         self.nodes.insert(id, Node::default());
      }
      let mut last = self.nodes.last_entry().expect("node");
      let master = *last.key();
      last.get_mut().push(master, id, fields);
      self.len += 1;
      self.last_id = id;
      self.entries_added += 1;
   }

   #[inline]
   fn start_key(&self, start: StreamId) -> StreamId {
      self
         .nodes
         .range(..=start)
         .next_back()
         .map(|(k, _)| *k)
         .unwrap_or(start)
   }

   fn scan(&self, start: StreamId, end: StreamId, count: usize, mut f: impl FnMut(Raw<'_>)) {
      if start > end || count == 0 {
         return;
      }
      let mut n = 0;
      for (master, node) in self.nodes.range(self.start_key(start)..=end) {
         for r in node.iter(*master) {
            if r.id > end {
               return;
            }
            if r.deleted || r.id < start {
               continue;
            }
            f(r);
            n += 1;
            if n >= count {
               return;
            }
         }
      }
   }

   fn scan_rev(&self, start: StreamId, end: StreamId, count: usize, mut f: impl FnMut(Raw<'_>)) {
      if start > end || count == 0 {
         return;
      }
      let mut n = 0;
      let mut buf: Vec<Raw<'_>> = Vec::with_capacity(NODE_MAX_ENTRIES as usize);
      for (master, node) in self.nodes.range(self.start_key(start)..=end).rev() {
         buf.clear();
         buf.extend(node.iter(*master).filter(|r| !r.deleted));
         for r in buf.iter().rev() {
            if r.id > end {
               continue;
            }
            if r.id < start {
               return;
            }
            f(*r);
            n += 1;
            if n >= count {
               return;
            }
         }
      }
   }

   pub fn range(&self, start: StreamId, end: StreamId, count: usize) -> Vec<StreamEntry> {
      let mut out = Vec::new();
      self.scan(start, end, count, |r| {
         out.push((r.id.to_string(), r.owned()))
      });
      out
   }

   pub fn rev_range(&self, start: StreamId, end: StreamId, count: usize) -> Vec<StreamEntry> {
      let mut out = Vec::new();
      self.scan_rev(start, end, count, |r| {
         out.push((r.id.to_string(), r.owned()))
      });
      out
   }

   pub fn after(&self, id: StreamId, count: usize) -> Vec<StreamEntry> {
      match id.next() {
         Some(start) => self.range(start, StreamId::MAX, count),
         None => Vec::new(),
      }
   }

   pub fn for_each(&self, mut f: impl FnMut(StreamId, FieldIter<'_>)) {
      for (master, node) in &self.nodes {
         for r in node.iter(*master) {
            if !r.deleted {
               f(r.id, r.fields());
            }
         }
      }
   }

   pub fn get(&self, id: StreamId) -> Option<Fields> {
      let mut hit = None;
      self.scan(id, id, 1, |r| hit = Some(r.owned()));
      hit
   }

   pub fn contains(&self, id: StreamId) -> bool {
      let mut hit = false;
      self.scan(id, id, 1, |_| hit = true);
      hit
   }

   pub fn first_entry(&self) -> Option<StreamEntry> {
      self.range(StreamId::MIN, StreamId::MAX, 1).pop()
   }

   pub fn last_entry(&self) -> Option<StreamEntry> {
      self.rev_range(StreamId::MIN, StreamId::MAX, 1).pop()
   }

   pub fn first_id(&self) -> Option<StreamId> {
      let mut hit = None;
      self.scan(StreamId::MIN, StreamId::MAX, 1, |r| hit = Some(r.id));
      hit
   }

   pub fn delete(&mut self, id: StreamId) -> bool {
      let Some((&key, node)) = self.nodes.range(..=id).next_back() else {
         return false;
      };
      let Some(flag) = node
         .iter(key)
         .find(|r| r.id == id && !r.deleted)
         .map(|r| r.flag)
      else {
         return false;
      };
      self.mark_deleted(key, flag);
      if id > self.max_deleted {
         self.max_deleted = id;
      }
      true
   }

   fn mark_deleted(&mut self, key: StreamId, flag: usize) {
      let node = self.nodes.get_mut(&key).expect("node");
      node.buf[flag] = 1;
      node.live -= 1;
      self.len -= 1;
      if node.live == 0 {
         self.nodes.remove(&key);
      }
   }

   fn pop_front_live(&mut self) -> Option<StreamId> {
      let (&key, node) = self.nodes.first_key_value()?;
      let r = node.iter(key).find(|r| !r.deleted)?;
      let (id, flag) = (r.id, r.flag);
      self.mark_deleted(key, flag);
      Some(id)
   }

   pub fn trim(&mut self, t: Trim) -> usize {
      let limit = if t.approx && t.limit > 0 {
         t.limit
      } else {
         usize::MAX
      };
      let mut removed = 0;
      while let Some((&key, node)) = self.nodes.first_key_value() {
         let live = node.live as usize;
         let node_last = node.iter(key).last().map(|r| r.id).unwrap_or(key);
         let whole = match t.by {
            TrimBy::MaxLen(max) => {
               if self.len <= max {
                  break;
               }
               self.len - live >= max
            }
            TrimBy::MinId(min) => {
               if self.first_id().is_none_or(|f| f >= min) {
                  break;
               }
               node_last < min
            }
         };
         if !whole || removed + live > limit {
            break;
         }
         self.nodes.pop_first();
         self.len -= live;
         removed += live;
      }
      if !t.approx {
         match t.by {
            TrimBy::MaxLen(max) => {
               while self.len > max && self.pop_front_live().is_some() {
                  removed += 1;
               }
            }
            TrimBy::MinId(min) => {
               while self.first_id().is_some_and(|f| f < min) && self.pop_front_live().is_some() {
                  removed += 1;
               }
            }
         }
      }
      removed
   }

   pub fn trim_maxlen(&mut self, maxlen: usize, approx: bool) -> usize {
      self.trim(Trim {
         by: TrimBy::MaxLen(maxlen),
         approx,
         limit: 0,
      })
   }

   pub fn raw_nodes(&self) -> impl Iterator<Item = (StreamId, &[u8], u32, u32)> {
      self
         .nodes
         .iter()
         .map(|(k, n)| (*k, n.buf.as_slice(), n.count, n.live))
   }

   pub fn push_raw_node(&mut self, master: StreamId, buf: Vec<u8>, count: u32, live: u32) -> bool {
      let Some(last) = Node::validate(&buf, master, count, live) else {
         return false;
      };
      if self
         .nodes
         .last_key_value()
         .is_some_and(|(k, n)| *k >= master || n.iter(*k).last().is_some_and(|r| r.id >= master))
      {
         return false;
      }
      self.nodes.insert(master, Node { buf, count, live });
      self.len += live as usize;
      if last > self.last_id {
         self.last_id = last;
      }
      true
   }

   fn group_mut(&mut self, group: &str) -> Result<&mut ConsumerGroup, &'static str> {
      self.groups.get_mut(group).ok_or(ERR_NOGROUP)
   }

   pub fn group_create(&mut self, group: &str, id: Option<StreamId>) -> Result<(), &'static str> {
      if self.groups.contains_key(group) {
         return Err(ERR_BUSYGROUP);
      }
      let last = id.unwrap_or(self.last_id);
      self
         .groups
         .insert(SmallStr::new(group), ConsumerGroup::new(last));
      Ok(())
   }

   pub fn group_setid(&mut self, group: &str, id: Option<StreamId>) -> Result<(), &'static str> {
      let last = id.unwrap_or(self.last_id);
      self.group_mut(group)?.last_delivered = last;
      Ok(())
   }

   pub fn read_group_new(
      &mut self,
      group: &str,
      consumer: &str,
      count: usize,
      noack: bool,
   ) -> Result<Claimed, &'static str> {
      let after = self.groups.get(group).ok_or(ERR_NOGROUP)?.last_delivered;
      let mut out: Claimed = Vec::new();
      if let Some(start) = after.next() {
         self.scan(start, StreamId::MAX, count, |r| {
            out.push((r.id, Some(r.owned())))
         });
      }
      let now = now_ms();
      let g = self.group_mut(group)?;
      let c = g.consumer(consumer, now);
      if !out.is_empty() {
         c.active_time = now;
      }
      if let Some(&(last, _)) = out.last() {
         g.last_delivered = last;
         g.entries_read = Some(g.entries_read.unwrap_or(0) + out.len() as u64);
      }
      if !noack {
         for (id, _) in &out {
            g.assign(*id, consumer, now, true);
         }
      }
      Ok(out)
   }

   pub fn read_group_history(
      &mut self,
      group: &str,
      consumer: &str,
      after: StreamId,
      count: usize,
   ) -> Result<Claimed, &'static str> {
      let now = now_ms();
      let g = self.group_mut(group)?;
      let c = g.consumer(consumer, now);
      let Some(start) = after.next() else {
         return Ok(Vec::new());
      };
      let take = if count == 0 { usize::MAX } else { count };
      let ids: Vec<StreamId> = c.pel.range(start..).take(take).copied().collect();
      Ok(ids.into_iter().map(|id| (id, self.get(id))).collect())
   }

   pub fn ack(&mut self, group: &str, ids: &[StreamId]) -> usize {
      let Some(g) = self.groups.get_mut(group) else {
         return 0;
      };
      ids.iter().filter(|id| g.drop_pending(**id)).count()
   }

   pub fn pending_summary(&self, group: &str) -> Result<PendingSummary, &'static str> {
      let g = self.groups.get(group).ok_or(ERR_NOGROUP)?;
      let bounds = match (g.pel.first_key_value(), g.pel.last_key_value()) {
         (Some((a, _)), Some((b, _))) => Some((*a, *b)),
         _ => None,
      };
      let mut per: Vec<(String, usize)> = g
         .consumers
         .iter()
         .filter(|(_, c)| !c.pel.is_empty())
         .map(|(n, c)| (n.to_string(), c.pel.len()))
         .collect();
      per.sort();
      Ok((g.pel.len(), bounds, per))
   }

   pub fn pending_range(
      &self,
      group: &str,
      min_idle: u64,
      start: StreamId,
      end: StreamId,
      count: usize,
      consumer: Option<&str>,
   ) -> Result<Vec<Pending>, &'static str> {
      let g = self.groups.get(group).ok_or(ERR_NOGROUP)?;
      if start > end {
         return Ok(Vec::new());
      }
      let now = now_ms();
      Ok(g
         .pel
         .range(start..=end)
         .filter(|(_, p)| consumer.is_none_or(|c| p.consumer.as_str() == c))
         .filter(|(_, p)| now.saturating_sub(p.delivery_time) >= min_idle)
         .take(count)
         .map(|(id, p)| Pending {
            id: *id,
            consumer: p.consumer.to_string(),
            idle: now.saturating_sub(p.delivery_time),
            deliveries: p.delivery_count,
         })
         .collect())
   }

   pub fn claim(
      &mut self,
      group: &str,
      consumer: &str,
      min_idle: u64,
      ids: &[StreamId],
      o: &ClaimOpts,
   ) -> Result<Claimed, &'static str> {
      let now = now_ms();
      let stamp = match (o.idle, o.time) {
         (Some(idle), _) => now.saturating_sub(idle),
         (None, Some(t)) => t,
         _ => now,
      };
      let exists: Vec<bool> = ids.iter().map(|id| self.contains(*id)).collect();
      let g = self.group_mut(group)?;
      if let Some(l) = o.last_id
         && l > g.last_delivered
      {
         g.last_delivered = l;
      }
      g.consumer(consumer, now);
      let mut claimed = Vec::new();
      for (id, live) in ids.iter().zip(exists) {
         let pending = g.pel.get(id);
         if pending.is_none() && !(o.force && live) {
            continue;
         }
         if !live {
            g.drop_pending(*id);
            continue;
         }
         if let Some(p) = pending
            && min_idle > 0
            && now.saturating_sub(p.delivery_time) < min_idle
         {
            continue;
         }
         g.assign(*id, consumer, stamp, !o.justid);
         if let Some(rc) = o.retry_count
            && let Some(p) = g.pel.get_mut(id)
         {
            p.delivery_count = rc;
         }
         claimed.push(*id);
      }
      if !claimed.is_empty()
         && let Some(c) = g.consumers.get_mut(consumer)
      {
         c.active_time = now;
      }
      Ok(claimed
         .into_iter()
         .map(|id| (id, if o.justid { None } else { self.get(id) }))
         .collect())
   }

   pub fn autoclaim(
      &mut self,
      group: &str,
      consumer: &str,
      min_idle: u64,
      start: StreamId,
      count: usize,
      justid: bool,
   ) -> Result<(StreamId, Claimed, Vec<StreamId>), &'static str> {
      let now = now_ms();
      let budget = count.saturating_mul(10).max(1);
      let scan: Vec<(StreamId, u64)> = {
         let g = self.groups.get(group).ok_or(ERR_NOGROUP)?;
         g.pel
            .range(start..)
            .take(budget + 1)
            .map(|(id, p)| (*id, p.delivery_time))
            .collect()
      };
      let mut next = StreamId::MIN;
      let mut picked = Vec::new();
      let mut deleted = Vec::new();
      for (seen, &(id, t)) in scan.iter().enumerate() {
         if seen >= budget || picked.len() >= count {
            next = id;
            break;
         }
         if !self.contains(id) {
            deleted.push(id);
            continue;
         }
         if now.saturating_sub(t) >= min_idle {
            picked.push(id);
         }
      }
      let g = self.group_mut(group)?;
      for id in &deleted {
         g.drop_pending(*id);
      }
      g.consumer(consumer, now);
      for id in &picked {
         g.assign(*id, consumer, now, !justid);
      }
      if !picked.is_empty()
         && let Some(c) = g.consumers.get_mut(consumer)
      {
         c.active_time = now;
      }
      let claimed = picked
         .into_iter()
         .map(|id| (id, if justid { None } else { self.get(id) }))
         .collect();
      Ok((next, claimed, deleted))
   }

   pub fn info(&self) -> StreamInfo {
      StreamInfo {
         length: self.len,
         nodes: self.nodes.len(),
         last_id: self.last_id,
         max_deleted: self.max_deleted,
         entries_added: self.entries_added,
         groups: self.groups.len(),
         first: self.first_entry(),
         last: self.last_entry(),
      }
   }

   pub fn groups_info(&self) -> Vec<GroupInfo> {
      let mut v: Vec<GroupInfo> = self
         .groups
         .iter()
         .map(|(n, g)| GroupInfo {
            name: n.to_string(),
            consumers: g.consumers.len(),
            pending: g.pel.len(),
            last_delivered: g.last_delivered,
            entries_read: g.entries_read,
            lag: g
               .entries_read
               .filter(|_| self.max_deleted <= g.last_delivered)
               .map(|r| self.entries_added.saturating_sub(r)),
         })
         .collect();
      v.sort_by(|a, b| a.name.cmp(&b.name));
      v
   }

   pub fn consumers_info(&self, group: &str) -> Result<Vec<ConsumerInfo>, &'static str> {
      let g = self.groups.get(group).ok_or(ERR_NOGROUP)?;
      let now = now_ms();
      let mut v: Vec<ConsumerInfo> = g
         .consumers
         .iter()
         .map(|(n, c)| ConsumerInfo {
            name: n.to_string(),
            pending: c.pel.len(),
            idle: now.saturating_sub(c.seen_time),
            inactive: if c.active_time == 0 {
               -1
            } else {
               now.saturating_sub(c.active_time) as i64
            },
         })
         .collect();
      v.sort_by(|a, b| a.name.cmp(&b.name));
      Ok(v)
   }
}

impl Store {
   fn stream_read<R>(
      &self,
      key: &str,
      f: impl FnOnce(&StreamData) -> R,
   ) -> Result<Option<R>, &'static str> {
      match self.data.get_ref(key) {
         None => Ok(None),
         Some(e) if e.is_expired() => Ok(None),
         Some(e) => match e.value.as_stream() {
            Some(s) => Ok(Some(f(s))),
            None => Err("WRONGTYPE"),
         },
      }
   }

   fn stream_write<R>(
      &self,
      key: &str,
      mut f: impl FnMut(&mut StreamData) -> Result<R, &'static str>,
   ) -> Result<Option<R>, &'static str> {
      let r = self.data.update_with(key, |val| {
         if val.is_expired() {
            return Ok(None);
         }
         match val.value.as_stream_mut() {
            Some(s) => f(s).map(Some),
            None => Err("WRONGTYPE"),
         }
      });
      r.unwrap_or(Ok(None))
   }

   fn stream_upsert<R>(
      &self,
      key: &str,
      mut f: impl FnMut(&mut StreamData) -> Result<R, &'static str>,
   ) -> Result<R, &'static str> {
      loop {
         let r = self.data.update_with(key, |val| {
            if val.is_expired() {
               let mut s = StreamData::new();
               let out = f(&mut s)?;
               val.value = FyroDB::Stream(Box::new(s));
               val.expires_ms = 0;
               return Ok(out);
            }
            match val.value.as_stream_mut() {
               Some(s) => f(s),
               None => Err("WRONGTYPE"),
            }
         });
         if let Some(r) = r {
            return r;
         }
         let mut s = StreamData::new();
         let out = f(&mut s)?;
         if self.data.insert_if_absent_str(key, StoreValue::stream(s)) {
            return Ok(out);
         }
      }
   }

   pub fn xadd<K: AsRef<str>, V: AsRef<str>>(
      &self,
      key: &str,
      id_str: &str,
      fields: &[(K, V)],
      trim: Option<Trim>,
      nomkstream: bool,
   ) -> Result<Option<String>, &'static str> {
      let spec = IdSpec::parse(id_str).ok_or(ERR_INVALID_ID)?;
      let apply = |s: &mut StreamData| {
         let id = s.add(spec, fields)?;
         if let Some(t) = trim {
            s.trim(t);
         }
         Ok(id)
      };
      let id = if nomkstream {
         match self.stream_write(key, apply)? {
            Some(id) => id,
            None => return Ok(None),
         }
      } else {
         self.stream_upsert(key, apply)?
      };
      self.blocking.signal(key);
      Ok(Some(id.to_string()))
   }

   pub fn xlen(&self, key: &str) -> Result<usize, &'static str> {
      Ok(self.stream_read(key, |s| s.len())?.unwrap_or(0))
   }

   pub fn xrange(
      &self,
      key: &str,
      start: &str,
      end: &str,
      count: usize,
   ) -> Result<Vec<StreamEntry>, &'static str> {
      let (Some(a), Some(b)) = (parse_range_start(start)?, parse_range_end(end)?) else {
         return Ok(Vec::new());
      };
      let limit = if count == 0 { usize::MAX } else { count };
      Ok(self
         .stream_read(key, |s| s.range(a, b, limit))?
         .unwrap_or_default())
   }

   pub fn xrevrange(
      &self,
      key: &str,
      end: &str,
      start: &str,
      count: usize,
   ) -> Result<Vec<StreamEntry>, &'static str> {
      let (Some(a), Some(b)) = (parse_range_start(start)?, parse_range_end(end)?) else {
         return Ok(Vec::new());
      };
      let limit = if count == 0 { usize::MAX } else { count };
      Ok(self
         .stream_read(key, |s| s.rev_range(a, b, limit))?
         .unwrap_or_default())
   }

   pub fn xread_after(
      &self,
      key: &str,
      after: StreamId,
      count: usize,
   ) -> Result<Vec<StreamEntry>, &'static str> {
      let limit = if count == 0 { usize::MAX } else { count };
      Ok(self
         .stream_read(key, |s| s.after(after, limit))?
         .unwrap_or_default())
   }

   pub fn xlast_id(&self, key: &str) -> Result<StreamId, &'static str> {
      Ok(self
         .stream_read(key, |s| s.last_id)?
         .unwrap_or(StreamId::MIN))
   }

   pub fn xtrim(&self, key: &str, trim: Trim) -> Result<usize, &'static str> {
      Ok(self.stream_write(key, |s| Ok(s.trim(trim)))?.unwrap_or(0))
   }

   pub fn xdel(&self, key: &str, ids: &[&str]) -> Result<usize, &'static str> {
      let parsed: Vec<StreamId> = ids
         .iter()
         .map(|s| StreamId::parse(s).ok_or(ERR_INVALID_ID))
         .collect::<Result<_, _>>()?;
      Ok(self
         .stream_write(key, |s| {
            Ok(parsed.iter().filter(|id| s.delete(**id)).count())
         })?
         .unwrap_or(0))
   }

   pub fn xgroup_create(
      &self,
      key: &str,
      group: &str,
      id: &str,
      mkstream: bool,
   ) -> Result<bool, &'static str> {
      let start = if id == "$" {
         None
      } else {
         Some(StreamId::parse(id).ok_or(ERR_INVALID_ID)?)
      };
      let create = |s: &mut StreamData| s.group_create(group, start).map(|_| true);
      if mkstream {
         self.stream_upsert(key, create)
      } else {
         self.stream_write(key, create)?.ok_or(ERR_NO_KEY_GROUP)
      }
   }

   pub fn xgroup_setid(&self, key: &str, group: &str, id: &str) -> Result<(), &'static str> {
      let target = if id == "$" {
         None
      } else {
         Some(StreamId::parse(id).ok_or(ERR_INVALID_ID)?)
      };
      self
         .stream_write(key, |s| s.group_setid(group, target))?
         .ok_or(ERR_NO_KEY_GROUP)
   }

   pub fn xgroup_destroy(&self, key: &str, group: &str) -> Result<bool, &'static str> {
      Ok(self
         .stream_write(key, |s| Ok(s.groups.remove(group).is_some()))?
         .unwrap_or(false))
   }

   pub fn xgroup_createconsumer(
      &self,
      key: &str,
      group: &str,
      consumer: &str,
   ) -> Result<bool, &'static str> {
      self
         .stream_write(key, |s| {
            let g = s.group_mut(group)?;
            if g.consumers.contains_key(consumer) {
               return Ok(false);
            }
            g.consumer(consumer, now_ms());
            Ok(true)
         })?
         .ok_or(ERR_NOGROUP)
   }

   pub fn xgroup_delconsumer(
      &self,
      key: &str,
      group: &str,
      consumer: &str,
   ) -> Result<usize, &'static str> {
      self
         .stream_write(key, |s| {
            let g = s.group_mut(group)?;
            let Some(c) = g.consumers.remove(consumer) else {
               return Ok(0);
            };
            for id in &c.pel {
               g.pel.remove(id);
            }
            Ok(c.pel.len())
         })?
         .ok_or(ERR_NOGROUP)
   }

   pub fn xreadgroup(
      &self,
      key: &str,
      group: &str,
      consumer: &str,
      after: Option<StreamId>,
      count: usize,
      noack: bool,
   ) -> Result<Claimed, &'static str> {
      let limit = if count == 0 { usize::MAX } else { count };
      self
         .stream_write(key, |s| match after {
            None => s.read_group_new(group, consumer, limit, noack),
            Some(a) => s.read_group_history(group, consumer, a, count),
         })?
         .ok_or(ERR_NOGROUP)
   }

   pub fn xack(&self, key: &str, group: &str, ids: &[&str]) -> Result<usize, &'static str> {
      let parsed: Vec<StreamId> = ids
         .iter()
         .map(|s| StreamId::parse(s).ok_or(ERR_INVALID_ID))
         .collect::<Result<_, _>>()?;
      Ok(self
         .stream_write(key, |s| Ok(s.ack(group, &parsed)))?
         .unwrap_or(0))
   }

   pub fn xpending_summary(&self, key: &str, group: &str) -> Result<PendingSummary, &'static str> {
      self
         .stream_read(key, |s| s.pending_summary(group))?
         .unwrap_or(Err(ERR_NOGROUP))
   }

   #[allow(clippy::too_many_arguments)]
   pub fn xpending_range(
      &self,
      key: &str,
      group: &str,
      min_idle: u64,
      start: StreamId,
      end: StreamId,
      count: usize,
      consumer: Option<&str>,
   ) -> Result<Vec<Pending>, &'static str> {
      self
         .stream_read(key, |s| {
            s.pending_range(group, min_idle, start, end, count, consumer)
         })?
         .unwrap_or(Err(ERR_NOGROUP))
   }

   pub fn xclaim(
      &self,
      key: &str,
      group: &str,
      consumer: &str,
      min_idle: u64,
      ids: &[StreamId],
      opts: &ClaimOpts,
   ) -> Result<Claimed, &'static str> {
      self
         .stream_write(key, |s| s.claim(group, consumer, min_idle, ids, opts))?
         .ok_or(ERR_NOGROUP)
   }

   #[allow(clippy::too_many_arguments)]
   pub fn xautoclaim(
      &self,
      key: &str,
      group: &str,
      consumer: &str,
      min_idle: u64,
      start: StreamId,
      count: usize,
      justid: bool,
   ) -> Result<(StreamId, Claimed, Vec<StreamId>), &'static str> {
      self
         .stream_write(key, |s| {
            s.autoclaim(group, consumer, min_idle, start, count, justid)
         })?
         .ok_or(ERR_NOGROUP)
   }

   pub fn xinfo_stream(&self, key: &str) -> Result<Option<StreamInfo>, &'static str> {
      self.stream_read(key, |s| s.info())
   }

   pub fn xinfo_groups(&self, key: &str) -> Result<Option<Vec<GroupInfo>>, &'static str> {
      self.stream_read(key, |s| s.groups_info())
   }

   pub fn xinfo_consumers(
      &self,
      key: &str,
      group: &str,
   ) -> Result<Vec<ConsumerInfo>, &'static str> {
      self
         .stream_read(key, |s| s.consumers_info(group))?
         .unwrap_or(Err(ERR_NOGROUP))
   }
}

#[cfg(test)]
mod tests {
   use super::*;

   fn f(k: &str, v: &str) -> Vec<(String, String)> {
      vec![(k.to_string(), v.to_string())]
   }

   #[test]
   fn packs_many_entries_per_node_and_ranges_correctly() {
      let mut s = StreamData::new();
      for i in 1..=1000u64 {
         s.add(IdSpec::Exact(StreamId::new(i, 0)), &f("k", &i.to_string()))
            .unwrap();
      }
      assert_eq!(s.len(), 1000);
      assert!(s.node_count() <= 12);
      let r = s.range(StreamId::new(250, 0), StreamId::new(260, 0), usize::MAX);
      assert_eq!(r.len(), 11);
      assert_eq!(r[0].0, "250-0");
      assert_eq!(r[0].1, f("k", "250"));
      let rr = s.rev_range(StreamId::new(250, 0), StreamId::new(260, 0), 3);
      let ids: Vec<_> = rr.iter().map(|e| e.0.as_str()).collect();
      assert_eq!(ids, ["260-0", "259-0", "258-0"]);
      assert_eq!(s.after(StreamId::new(998, 0), 10).len(), 2);
   }

   #[test]
   fn rejects_non_increasing_ids() {
      let mut s = StreamData::new();
      assert_eq!(
         s.add(IdSpec::Exact(StreamId::MIN), &f("a", "b")),
         Err(ERR_ID_ZERO)
      );
      s.add(IdSpec::Exact(StreamId::new(5, 1)), &f("a", "b"))
         .unwrap();
      assert_eq!(
         s.add(IdSpec::Exact(StreamId::new(5, 1)), &f("a", "b")),
         Err(ERR_ID_TOO_SMALL)
      );
      assert_eq!(
         s.add(IdSpec::AutoSeq(5), &f("a", "b")),
         Ok(StreamId::new(5, 2))
      );
      assert_eq!(
         s.add(IdSpec::AutoSeq(4), &f("a", "b")),
         Err(ERR_ID_TOO_SMALL)
      );
      assert!(s.add(IdSpec::Auto, &f("a", "b")).unwrap() > StreamId::new(5, 2));
   }

   #[test]
   fn delete_and_trim() {
      let mut s = StreamData::new();
      for i in 1..=500u64 {
         s.append(StreamId::new(i, 0), &f("k", "v"));
      }
      assert!(s.delete(StreamId::new(3, 0)));
      assert!(!s.delete(StreamId::new(3, 0)));
      assert_eq!(s.len(), 499);
      assert_eq!(s.max_deleted, StreamId::new(3, 0));
      assert!(s.get(StreamId::new(3, 0)).is_none());
      let approx = s.trim(Trim {
         by: TrimBy::MaxLen(250),
         approx: true,
         limit: 0,
      });
      assert!(s.len() >= 250 && approx > 0);
      assert_eq!(approx % 100, 99);
      s.trim_maxlen(250, false);
      assert_eq!(s.len(), 250);
      assert_eq!(s.first_id(), Some(StreamId::new(251, 0)));
      s.trim(Trim {
         by: TrimBy::MinId(StreamId::new(400, 0)),
         approx: false,
         limit: 0,
      });
      assert_eq!(s.first_id(), Some(StreamId::new(400, 0)));
      assert_eq!(s.len(), 101);
   }

   #[test]
   fn consumer_group_lifecycle() {
      let mut s = StreamData::new();
      for i in 1..=10u64 {
         s.append(StreamId::new(i, 0), &f("n", &i.to_string()));
      }
      s.group_create("g", Some(StreamId::MIN)).unwrap();
      assert_eq!(s.group_create("g", None), Err(ERR_BUSYGROUP));
      let a = s.read_group_new("g", "alice", 4, false).unwrap();
      assert_eq!(a.len(), 4);
      let b = s.read_group_new("g", "bob", 100, false).unwrap();
      assert_eq!(b.len(), 6);
      assert!(s.read_group_new("g", "bob", 100, false).unwrap().is_empty());
      let (n, bounds, per) = s.pending_summary("g").unwrap();
      assert_eq!(n, 10);
      assert_eq!(bounds, Some((StreamId::new(1, 0), StreamId::new(10, 0))));
      assert_eq!(per, vec![("alice".to_string(), 4), ("bob".to_string(), 6)]);
      let hist = s
         .read_group_history("g", "alice", StreamId::MIN, 0)
         .unwrap();
      assert_eq!(hist.len(), 4);
      assert_eq!(
         s.ack(
            "g",
            &[
               StreamId::new(1, 0),
               StreamId::new(1, 0),
               StreamId::new(99, 0)
            ]
         ),
         1
      );
      let opts = ClaimOpts {
         idle: None,
         time: None,
         retry_count: None,
         force: false,
         justid: false,
         last_id: None,
      };
      let c = s
         .claim("g", "bob", 0, &[StreamId::new(2, 0)], &opts)
         .unwrap();
      assert_eq!(c.len(), 1);
      assert_eq!(s.groups["g"].pel[&StreamId::new(2, 0)].delivery_count, 2);
      assert_eq!(
         s.pending_summary("g").unwrap().2,
         vec![("alice".to_string(), 2), ("bob".to_string(), 7)]
      );
      s.delete(StreamId::new(3, 0));
      let (next, got, deleted) = s
         .autoclaim("g", "carol", 0, StreamId::MIN, 2, false)
         .unwrap();
      assert_eq!(deleted, vec![StreamId::new(3, 0)]);
      assert_eq!(got.len(), 2);
      assert!(next > StreamId::MIN);
      assert!(!s.groups["g"].pel.contains_key(&StreamId::new(3, 0)));
   }

   #[test]
   fn raw_nodes_round_trip_and_reject_garbage() {
      let mut s = StreamData::new();
      for i in 1..=250u64 {
         s.append(StreamId::new(i, i), &[("f", "v"), ("g", "é")]);
      }
      s.delete(StreamId::new(7, 7));
      let mut t = StreamData::new();
      for (m, buf, c, l) in s.raw_nodes() {
         assert!(t.push_raw_node(m, buf.to_vec(), c, l));
      }
      assert_eq!(t.len(), s.len());
      assert_eq!(t.last_id, s.last_id);
      assert_eq!(
         t.range(StreamId::MIN, StreamId::MAX, usize::MAX),
         s.range(StreamId::MIN, StreamId::MAX, usize::MAX)
      );
      let mut u = StreamData::new();
      assert!(!u.push_raw_node(StreamId::new(1, 0), vec![0, 0xff], 1, 1));
      assert!(!u.push_raw_node(
         StreamId::new(1, 0),
         vec![0, 0, 0, 1, 2, 0xc3, 0x28, 0],
         1,
         1
      ));
   }

   #[test]
   fn range_bounds_parse_exclusive_and_partial_ids() {
      assert_eq!(parse_range_start("(5-3"), Ok(Some(StreamId::new(5, 4))));
      assert_eq!(
         parse_range_end("(5-0"),
         Ok(Some(StreamId::new(4, u64::MAX)))
      );
      assert_eq!(parse_range_end("7"), Ok(Some(StreamId::new(7, u64::MAX))));
      assert_eq!(parse_range_start("7"), Ok(Some(StreamId::new(7, 0))));
      assert!(parse_range_start("x").is_err());
   }
}
