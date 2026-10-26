use super::small_str::SmallStr;
use foldhash::{HashMap, HashMapExt, HashSet};

pub(crate) const COMPACT_THRESHOLD: usize = 128;

#[derive(Clone)]
pub enum HashInner {
   Compact(Vec<SmallStr>),
   Full(Box<HashMap<SmallStr, SmallStr>>),
}

#[derive(Clone)]
pub enum SetInner {
   Integers(Vec<i64>),
   Compact(Vec<SmallStr>),
   Full(Box<HashSet<SmallStr>>),
}

#[inline]
fn compact_find(v: &[SmallStr], field: &str) -> Result<usize, usize> {
   let n = v.len() / 2;
   if n <= 4 {
      for i in 0..n {
         let f = unsafe { v.get_unchecked(i * 2).as_str() };
         if f == field {
            return Ok(i);
         }
         if f > field {
            return Err(i);
         }
      }
      return Err(n);
   }
   let mut lo = 0usize;
   let mut hi = n;
   while lo < hi {
      let mid = lo + (hi - lo) / 2;
      match v[mid * 2].as_str().cmp(field) {
         std::cmp::Ordering::Less => lo = mid + 1,
         std::cmp::Ordering::Equal => return Ok(mid),
         std::cmp::Ordering::Greater => hi = mid,
      }
   }
   Err(lo)
}

impl Default for HashInner {
   fn default() -> Self {
      Self::new()
   }
}

impl HashInner {
   #[inline]
   pub fn new() -> Self {
      Self::Compact(Vec::new())
   }

   #[inline]
   pub fn with_capacity(field_count: usize) -> Self {
      if field_count <= COMPACT_THRESHOLD {
         Self::Compact(Vec::with_capacity(field_count * 2))
      } else {
         Self::Full(Box::new(HashMap::with_capacity_and_hasher(
            field_count,
            Default::default(),
         )))
      }
   }

   #[inline]
   pub fn get(&self, field: &str) -> Option<&SmallStr> {
      match self {
         Self::Compact(v) => compact_find(v, field).ok().map(|i| &v[i * 2 + 1]),
         Self::Full(m) => m.get(field),
      }
   }

   #[inline]
   pub fn insert(&mut self, field: String, value: String) -> bool {
      self.insert_small(SmallStr::from_string(field), SmallStr::from_string(value))
   }

   /// One scan finds-or-inserts; overwrites avoid two String temporaries.
   #[inline]
   pub fn insert_ref(&mut self, field: &str, value: &str) -> bool {
      self.insert_field(field, SmallStr::new(value))
   }

   #[inline]
   pub fn insert_bytes(&mut self, field: &str, value: &[u8]) -> bool {
      self.insert_field(field, SmallStr::from_slice(value))
   }

   pub fn insert_field(&mut self, field: &str, value: SmallStr) -> bool {
      match self {
         Self::Compact(v) => match compact_find(v, field) {
            Ok(pair_idx) => {
               v[pair_idx * 2 + 1] = value;
               false
            }
            Err(pos) => {
               let idx = pos * 2;
               if idx == v.len() {
                  v.push(SmallStr::new(field));
                  v.push(value);
               } else {
                  v.insert(idx, SmallStr::new(field));
                  v.insert(idx + 1, value);
               }
               if v.len() / 2 > COMPACT_THRESHOLD {
                  self.promote_hash();
               }
               true
            }
         },
         Self::Full(m) => match m.get_mut(field) {
            Some(slot) => {
               *slot = value;
               false
            }
            None => {
               m.insert(SmallStr::new(field), value);
               true
            }
         },
      }
   }

   pub fn insert_small(&mut self, field: SmallStr, value: SmallStr) -> bool {
      match self {
         Self::Compact(v) => match compact_find(v, field.as_str()) {
            Ok(pair_idx) => {
               v[pair_idx * 2 + 1] = value;
               false
            }
            Err(pos) => {
               let idx = pos * 2;
               v.insert(idx, field);
               v.insert(idx + 1, value);
               if v.len() / 2 > COMPACT_THRESHOLD {
                  self.promote_hash();
               }
               true
            }
         },
         Self::Full(m) => m.insert(field, value).is_none(),
      }
   }

   #[inline]
   pub fn remove(&mut self, field: &str) -> Option<String> {
      match self {
         Self::Compact(v) => match compact_find(v, field) {
            Ok(pair_idx) => {
               let idx = pair_idx * 2;
               v.remove(idx);
               let val = v.remove(idx).into_string();
               if v.capacity() > v.len().saturating_mul(2).max(8) {
                  v.shrink_to_fit();
               }
               Some(val)
            }
            Err(_) => None,
         },
         Self::Full(m) => {
            let out = m.remove(field).map(|s| s.into_string());
            if m.len() <= COMPACT_THRESHOLD / 2 {
               self.demote_hash();
            }
            out
         }
      }
   }

   #[inline]
   pub fn len(&self) -> usize {
      match self {
         Self::Compact(v) => v.len() / 2,
         Self::Full(m) => m.len(),
      }
   }

   #[inline]
   pub fn is_empty(&self) -> bool {
      self.len() == 0
   }

   #[inline]
   pub fn contains_key(&self, field: &str) -> bool {
      self.get(field).is_some()
   }

   pub fn keys(&self) -> Vec<&SmallStr> {
      match self {
         Self::Compact(v) => v.iter().step_by(2).collect(),
         Self::Full(m) => m.keys().collect(),
      }
   }

   pub fn values(&self) -> Vec<&SmallStr> {
      match self {
         Self::Compact(v) => v.iter().skip(1).step_by(2).collect(),
         Self::Full(m) => m.values().collect(),
      }
   }

   pub fn iter(&self) -> HashIter<'_> {
      match self {
         Self::Compact(v) => HashIter::Compact(v, 0),
         Self::Full(m) => HashIter::Full(m.iter()),
      }
   }

   fn promote_hash(&mut self) {
      if let Self::Compact(v) = self {
         let mut map = HashMap::with_capacity(v.len() / 2);
         let mut i = 0;
         while i < v.len() {
            let val = std::mem::take(&mut v[i + 1]);
            let key = std::mem::take(&mut v[i]);
            map.insert(key, val);
            i += 2;
         }
         *self = Self::Full(Box::new(map));
      }
   }

   fn demote_hash(&mut self) {
      let Self::Full(m) = self else { return };
      if m.len() > COMPACT_THRESHOLD / 2 {
         return;
      }
      let mut pairs: Vec<(SmallStr, SmallStr)> = std::mem::take(m).into_iter().collect();
      pairs.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
      let mut v = Vec::with_capacity(pairs.len() * 2);
      for (k, val) in pairs {
         v.push(k);
         v.push(val);
      }
      *self = Self::Compact(v);
   }
}

pub enum HashIter<'a> {
   Compact(&'a Vec<SmallStr>, usize),
   Full(std::collections::hash_map::Iter<'a, SmallStr, SmallStr>),
}

impl<'a> Iterator for HashIter<'a> {
   type Item = (&'a SmallStr, &'a SmallStr);
   fn next(&mut self) -> Option<Self::Item> {
      match self {
         Self::Compact(v, i) => {
            if *i >= v.len() {
               return None;
            }
            let pair = (&v[*i], &v[*i + 1]);
            *i += 2;
            Some(pair)
         }
         Self::Full(it) => it.next(),
      }
   }
}

impl Default for SetInner {
   fn default() -> Self {
      Self::new()
   }
}

impl SetInner {
   #[inline]
   pub fn new() -> Self {
      Self::Integers(Vec::new())
   }

   pub fn from_strings(values: impl IntoIterator<Item = String>) -> Self {
      let mut set = Self::new();
      for value in values {
         set.insert(value);
      }
      set
   }

   #[inline]
   pub fn insert(&mut self, member: String) -> bool {
      self.insert_small(SmallStr::from_string(member))
   }

   #[inline]
   pub fn insert_str(&mut self, member: &str) -> bool {
      self.insert_small(SmallStr::new(member))
   }

   #[inline]
   pub fn insert_small(&mut self, member: SmallStr) -> bool {
      match self {
         Self::Integers(v) => {
            if let Some(n) = canonical_i64(member.as_str()) {
               match v.binary_search(&n) {
                  Ok(_) => false,
                  Err(pos) => {
                     v.insert(pos, n);
                     true
                  }
               }
            } else {
               let mut strings = Vec::with_capacity(v.len() + 1);
               strings.extend(v.drain(..).map(|n| {
                  let mut buf = [0u8; 20];
                  let s = crate::storage::value::write_int_to(&mut buf, n);
                  SmallStr::new(s)
               }));
               strings.push(member);
               strings.sort_by(|a, b| a.as_str().cmp(b.as_str()));
               *self = Self::Compact(strings);
               true
            }
         }
         Self::Compact(v) => match v.binary_search_by(|m| m.as_str().cmp(member.as_str())) {
            Ok(_) => false,
            Err(pos) => {
               v.insert(pos, member);
               if v.len() > COMPACT_THRESHOLD {
                  self.promote_set();
               }
               true
            }
         },
         Self::Full(s) => s.insert(member),
      }
   }

   #[inline]
   pub fn remove(&mut self, member: &str) -> bool {
      match self {
         Self::Integers(v) => canonical_i64(member)
            .and_then(|n| v.binary_search(&n).ok())
            .map(|pos| {
               v.remove(pos);
               true
            })
            .unwrap_or(false),
         Self::Compact(v) => match v.binary_search_by(|m| m.as_str().cmp(member)) {
            Ok(pos) => {
               v.remove(pos);
               if v.capacity() > v.len().saturating_mul(2).max(8) {
                  v.shrink_to_fit();
               }
               true
            }
            Err(_) => false,
         },
         Self::Full(s) => {
            let removed = s.remove(member);
            if s.len() <= COMPACT_THRESHOLD / 2 {
               self.demote_set();
            }
            removed
         }
      }
   }

   #[inline]
   pub fn contains(&self, member: &str) -> bool {
      match self {
         Self::Integers(v) => canonical_i64(member).is_some_and(|n| v.binary_search(&n).is_ok()),
         Self::Compact(v) => v.binary_search_by(|m| m.as_str().cmp(member)).is_ok(),
         Self::Full(s) => s.contains(member),
      }
   }

   #[inline]
   pub fn len(&self) -> usize {
      match self {
         Self::Integers(v) => v.len(),
         Self::Compact(v) => v.len(),
         Self::Full(s) => s.len(),
      }
   }

   #[inline]
   pub fn is_empty(&self) -> bool {
      self.len() == 0
   }

   pub fn iter(&self) -> SetIter<'_> {
      match self {
         Self::Integers(v) => SetIter::Integers(v.iter()),
         Self::Compact(v) => SetIter::Compact(v.iter()),
         Self::Full(s) => SetIter::Full(s.iter()),
      }
   }

   fn promote_set(&mut self) {
      if let Self::Compact(v) = self {
         let mut set: HashSet<SmallStr> =
            HashSet::with_capacity_and_hasher(v.len(), Default::default());
         for item in v.drain(..) {
            set.insert(item);
         }
         *self = Self::Full(Box::new(set));
      }
   }

   fn demote_set(&mut self) {
      let Self::Full(s) = self else { return };
      if s.len() > COMPACT_THRESHOLD / 2 {
         return;
      }
      let mut v: Vec<SmallStr> = std::mem::take(s).into_iter().collect();
      v.sort_by(|a, b| a.as_str().cmp(b.as_str()));
      *self = Self::Compact(v);
   }
}

pub enum SetIter<'a> {
   Integers(std::slice::Iter<'a, i64>),
   Compact(std::slice::Iter<'a, SmallStr>),
   Full(std::collections::hash_set::Iter<'a, SmallStr>),
}

impl<'a> Iterator for SetIter<'a> {
   type Item = SetMemberRef<'a>;
   fn next(&mut self) -> Option<Self::Item> {
      match self {
         Self::Integers(it) => it.next().copied().map(SetMemberRef::Integer),
         Self::Compact(it) => it.next().map(SetMemberRef::String),
         Self::Full(it) => it.next().map(SetMemberRef::String),
      }
   }
}

#[derive(Clone, Copy)]
pub enum SetMemberRef<'a> {
   Integer(i64),
   String(&'a SmallStr),
}

impl SetMemberRef<'_> {
   pub fn len(self) -> usize {
      match self {
         Self::String(s) => s.len(),
         Self::Integer(n) => {
            let mut buf = [0u8; 20];
            crate::storage::value::write_int_to(&mut buf, n).len()
         }
      }
   }
   pub fn is_empty(self) -> bool {
      self.len() == 0
   }
   pub fn matches(self, value: &str) -> bool {
      match self {
         Self::Integer(n) => canonical_i64(value) == Some(n),
         Self::String(s) => s.as_str() == value,
      }
   }

   /// Writes the member as a RESP bulk: SmallStr directly, integers through
   /// a stack buffer so no String temporary is needed.
   pub fn write_bulk_to(self, out: &mut Vec<u8>) {
      match self {
         Self::String(s) => crate::utils::resp::write_bulk(out, s.as_str()),
         Self::Integer(n) => {
            let mut buf = [0u8; 20];
            let s = crate::storage::value::write_int_to(&mut buf, n);
            crate::utils::resp::write_bulk(out, s);
         }
      }
   }
}

impl std::fmt::Display for SetMemberRef<'_> {
   fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      match self {
         Self::Integer(n) => n.fmt(f),
         Self::String(s) => s.fmt(f),
      }
   }
}

#[inline]
pub fn canonical_i64(value: &str) -> Option<i64> {
   let bytes = value.as_bytes();
   if bytes.is_empty() {
      return None;
   }
   if bytes[0] == b'0' && bytes.len() > 1 {
      return None;
   }
   if bytes.len() >= 2 && bytes[0] == b'-' && bytes[1] == b'0' {
      return None;
   }
   if bytes[0] == b'+' {
      return None;
   }
   value.parse::<i64>().ok()
}
