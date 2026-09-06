use super::small_str::SmallStr;
use foldhash::{HashMap, HashMapExt, HashSet};
use std::collections::VecDeque;

pub(crate) const COMPACT_THRESHOLD: usize = 64;

#[derive(Clone)]
pub enum HashInner {
    Compact(Vec<SmallStr>),
    Full(Box<HashMap<SmallStr, SmallStr>>),
}

#[derive(Clone)]
pub enum ListInner {
    Compact(VecDeque<SmallStr>),
    Full(Box<VecDeque<SmallStr>>),
}

#[derive(Clone)]
pub enum SetInner {
    Integers(Vec<i64>),
    Compact(Vec<SmallStr>),
    Full(Box<HashSet<SmallStr>>),
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
    pub fn get(&self, field: &str) -> Option<&SmallStr> {
        match self {
            Self::Compact(v) => {
                let mut i = 0;
                while i < v.len() {
                    if v[i] == field {
                        return Some(&v[i + 1]);
                    }
                    i += 2;
                }
                None
            }
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
        self.insert_small(SmallStr::new(field), SmallStr::new(value))
    }

    #[inline]
    pub fn insert_small(&mut self, field: SmallStr, value: SmallStr) -> bool {
        match self {
            Self::Compact(v) => {
                let mut i = 0;
                while i < v.len() {
                    if v[i] == field {
                        v[i + 1] = value;
                        return false;
                    }
                    i += 2;
                }
                v.push(field);
                v.push(value);
                if v.len() / 2 > COMPACT_THRESHOLD {
                    self.promote_hash();
                }
                true
            }
            Self::Full(m) => m.insert(field, value).is_none(),
        }
    }

    #[inline]
    pub fn remove(&mut self, field: &str) -> Option<String> {
        match self {
            Self::Compact(v) => {
                let mut i = 0;
                while i < v.len() {
                    if v[i] == field {
                        let len = v.len();
                        v.swap(i, len - 2);
                        v.swap(i + 1, len - 1);
                        v.pop();
                        let out = v.pop().map(|s| s.into_string());
                        if v.capacity() > v.len().saturating_mul(2).max(8) {
                            v.shrink_to_fit();
                        }
                        return out;
                    }
                    i += 2;
                }
                None
            }
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
        let mut v = Vec::with_capacity(m.len() * 2);
        for (k, val) in std::mem::take(m).into_iter() {
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
                    strings.extend(v.drain(..).map(|n| SmallStr::from_string(n.to_string())));
                    strings.push(member);
                    *self = Self::Compact(strings);
                    true
                }
            }
            Self::Compact(v) => {
                if v.contains(&member) {
                    return false;
                }
                v.push(member);
                if v.len() > COMPACT_THRESHOLD {
                    self.promote_set();
                }
                true
            }
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
            Self::Compact(v) => {
                if let Some(pos) = v.iter().position(|m| m == member) {
                    v.swap_remove(pos);
                    if v.capacity() > v.len().saturating_mul(2).max(8) {
                        v.shrink_to_fit();
                    }
                    true
                } else {
                    false
                }
            }
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
            Self::Compact(v) => v.iter().any(|m| m == member),
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
        let v = std::mem::take(s).into_iter().collect();
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
        self.to_string().len()
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

pub fn canonical_i64(value: &str) -> Option<i64> {
    let n = value.parse::<i64>().ok()?;
    (n.to_string() == value).then_some(n)
}

impl Default for ListInner {
    fn default() -> Self {
        Self::new()
    }
}

impl ListInner {
    #[inline]
    pub fn new() -> Self {
        Self::Compact(VecDeque::new())
    }

    #[inline]
    pub fn push_front(&mut self, value: String) {
        let value = SmallStr::from_string(value);
        match self {
            Self::Compact(v) => {
                v.push_front(value);
                if v.len() > COMPACT_THRESHOLD {
                    self.promote_list();
                }
            }
            Self::Full(v) => v.push_front(value),
        }
    }

    #[inline]
    pub fn push_back(&mut self, value: String) {
        let value = SmallStr::from_string(value);
        match self {
            Self::Compact(v) => {
                v.push_back(value);
                if v.len() > COMPACT_THRESHOLD {
                    self.promote_list();
                }
            }
            Self::Full(v) => v.push_back(value),
        }
    }

    #[inline]
    pub fn pop_front(&mut self) -> Option<String> {
        match self {
            Self::Compact(v) => v.pop_front().map(SmallStr::into_string),
            Self::Full(v) => {
                let out = v.pop_front().map(SmallStr::into_string);
                if v.len() <= COMPACT_THRESHOLD / 2 {
                    self.demote_list();
                }
                out
            }
        }
    }

    #[inline]
    pub fn pop_back(&mut self) -> Option<String> {
        match self {
            Self::Compact(v) => v.pop_back().map(SmallStr::into_string),
            Self::Full(v) => {
                let out = v.pop_back().map(SmallStr::into_string);
                if v.len() <= COMPACT_THRESHOLD / 2 {
                    self.demote_list();
                }
                out
            }
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Compact(v) => v.len(),
            Self::Full(v) => v.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn deque(&self) -> &VecDeque<SmallStr> {
        match self {
            Self::Compact(v) => v,
            Self::Full(v) => v,
        }
    }

    pub fn deque_mut(&mut self) -> &mut VecDeque<SmallStr> {
        match self {
            Self::Compact(v) => v,
            Self::Full(v) => v,
        }
    }

    fn promote_list(&mut self) {
        if let Self::Compact(v) = self {
            let boxed = Box::new(std::mem::take(v));
            *self = Self::Full(boxed);
        }
    }

    fn demote_list(&mut self) {
        let Self::Full(v) = self else { return };
        if v.len() > COMPACT_THRESHOLD / 2 {
            return;
        }
        *self = Self::Compact(std::mem::take(v));
    }
}
