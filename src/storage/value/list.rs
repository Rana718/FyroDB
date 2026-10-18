use super::quicklist::QuickList;
use super::small_str::SmallStr;
use std::collections::VecDeque;

const SMALL_MAX: usize = 128;
const SMALL_BACK: usize = 32;

#[derive(Clone)]
pub enum ListValue {
   Small(VecDeque<SmallStr>),
   Packed(QuickList),
}

impl Default for ListValue {
   fn default() -> Self {
      Self::new()
   }
}

enum Either<A, B> {
   A(A),
   B(B),
}

impl<'a, A, B> Iterator for Either<A, B>
where
   A: Iterator<Item = &'a str>,
   B: Iterator<Item = &'a str>,
{
   type Item = &'a str;

   #[inline]
   fn next(&mut self) -> Option<&'a str> {
      match self {
         Self::A(a) => a.next(),
         Self::B(b) => b.next(),
      }
   }

   fn size_hint(&self) -> (usize, Option<usize>) {
      match self {
         Self::A(a) => a.size_hint(),
         Self::B(b) => b.size_hint(),
      }
   }
}

impl<'a, A, B> DoubleEndedIterator for Either<A, B>
where
   A: DoubleEndedIterator<Item = &'a str>,
   B: DoubleEndedIterator<Item = &'a str>,
{
   #[inline]
   fn next_back(&mut self) -> Option<&'a str> {
      match self {
         Self::A(a) => a.next_back(),
         Self::B(b) => b.next_back(),
      }
   }
}

impl ListValue {
   #[inline]
   pub fn new() -> Self {
      Self::Small(VecDeque::new())
   }

   pub fn from_strs<'a>(items: impl IntoIterator<Item = &'a str>) -> Self {
      let mut l = Self::new();
      for s in items {
         l.push_back(s);
      }
      l
   }

   #[inline]
   pub fn len(&self) -> usize {
      match self {
         Self::Small(v) => v.len(),
         Self::Packed(q) => q.len(),
      }
   }

   #[inline]
   pub fn is_empty(&self) -> bool {
      self.len() == 0
   }

   pub fn memory_bytes(&self) -> usize {
      match self {
         Self::Small(v) => {
            v.capacity() * size_of::<SmallStr>()
               + v.iter()
                  .filter(|s| s.len() > 23)
                  .map(|s| s.len())
                  .sum::<usize>()
         }
         Self::Packed(q) => q.memory_bytes(),
      }
   }

   #[inline]
   fn grow_check(&mut self) {
      if let Self::Small(v) = self
         && v.len() > SMALL_MAX
      {
         let q = QuickList::from_strs(v.iter().map(|s| s.as_str()));
         *self = Self::Packed(q);
      }
   }

   #[inline]
   fn shrink_check(&mut self) {
      if let Self::Packed(q) = self
         && q.len() <= SMALL_BACK
      {
         let v: VecDeque<SmallStr> = q.iter().map(SmallStr::new).collect();
         *self = Self::Small(v);
      }
   }

   #[inline]
   pub fn push_back(&mut self, s: &str) {
      match self {
         Self::Small(v) => {
            v.push_back(SmallStr::new(s));
            if v.len() > SMALL_MAX {
               self.grow_check();
            }
         }
         Self::Packed(q) => q.push_back(s),
      }
   }

   #[inline]
   pub fn push_front(&mut self, s: &str) {
      match self {
         Self::Small(v) => {
            v.push_front(SmallStr::new(s));
            if v.len() > SMALL_MAX {
               self.grow_check();
            }
         }
         Self::Packed(q) => q.push_front(s),
      }
   }

   #[inline]
   pub fn pop_front_with<R>(&mut self, f: impl FnOnce(&str) -> R) -> Option<R> {
      match self {
         Self::Small(v) => v.pop_front().map(|s| f(s.as_str())),
         Self::Packed(q) => {
            let r = q.pop_front_with(f);
            self.shrink_check();
            r
         }
      }
   }

   #[inline]
   pub fn pop_back_with<R>(&mut self, f: impl FnOnce(&str) -> R) -> Option<R> {
      match self {
         Self::Small(v) => v.pop_back().map(|s| f(s.as_str())),
         Self::Packed(q) => {
            let r = q.pop_back_with(f);
            self.shrink_check();
            r
         }
      }
   }

   pub fn pop_front(&mut self) -> Option<String> {
      match self {
         Self::Small(v) => v.pop_front().map(SmallStr::into_string),
         Self::Packed(_) => self.pop_front_with(str::to_string),
      }
   }

   pub fn pop_back(&mut self) -> Option<String> {
      match self {
         Self::Small(v) => v.pop_back().map(SmallStr::into_string),
         Self::Packed(_) => self.pop_back_with(str::to_string),
      }
   }

   pub fn get(&self, idx: usize) -> Option<&str> {
      match self {
         Self::Small(v) => v.get(idx).map(|s| s.as_str()),
         Self::Packed(q) => q.get(idx),
      }
   }

   pub fn iter(&self) -> impl DoubleEndedIterator<Item = &str> + '_ {
      match self {
         Self::Small(v) => Either::A(v.iter().map(|s| s.as_str())),
         Self::Packed(q) => Either::B(q.iter()),
      }
   }

   pub fn range(&self, start: usize, count: usize) -> impl Iterator<Item = &str> + '_ {
      match self {
         Self::Small(v) => {
            let end = start.saturating_add(count).min(v.len());
            Either::A(v.range(start.min(end)..end).map(|s| s.as_str()))
         }
         Self::Packed(q) => Either::B(q.range(start, count)),
      }
   }

   pub fn set(&mut self, idx: usize, value: &str) -> bool {
      match self {
         Self::Small(v) => match v.get_mut(idx) {
            Some(slot) => {
               *slot = SmallStr::new(value);
               true
            }
            None => false,
         },
         Self::Packed(q) => q.set(idx, value),
      }
   }

   pub fn insert(&mut self, idx: usize, value: &str) {
      match self {
         Self::Small(v) => {
            v.insert(idx.min(v.len()), SmallStr::new(value));
            self.grow_check();
         }
         Self::Packed(q) => q.insert(idx, value),
      }
   }

   pub fn position(&self, value: &str) -> Option<usize> {
      self.iter().position(|v| v == value)
   }

   pub fn remove_matching(&mut self, value: &str, limit: usize, from_back: bool) -> usize {
      let removed = match self {
         Self::Small(v) => {
            let mut removed = 0;
            if from_back {
               let mut i = v.len();
               while i > 0 && removed < limit {
                  i -= 1;
                  if v[i] == value {
                     v.remove(i);
                     removed += 1;
                  }
               }
            } else {
               let mut i = 0;
               while i < v.len() && removed < limit {
                  if v[i] == value {
                     v.remove(i);
                     removed += 1;
                  } else {
                     i += 1;
                  }
               }
            }
            removed
         }
         Self::Packed(q) => q.remove_matching(value, limit, from_back),
      };
      self.shrink_check();
      removed
   }

   pub fn trim(&mut self, start: usize, keep: usize) {
      match self {
         Self::Small(v) => {
            v.drain(..start.min(v.len()));
            v.truncate(keep);
            if v.capacity() > v.len().saturating_mul(2).max(16) {
               v.shrink_to_fit();
            }
         }
         Self::Packed(q) => q.trim(start, keep),
      }
      self.shrink_check();
   }

   pub fn clear(&mut self) {
      *self = Self::new();
   }

   pub fn shrink_to_fit(&mut self) {
      match self {
         Self::Small(v) => {
            if v.capacity() > v.len().saturating_mul(2).max(16) {
               v.shrink_to_fit();
            }
         }
         Self::Packed(q) => q.shrink_to_fit(),
      }
   }
}

#[cfg(test)]
#[path = "list_tests.rs"]
mod tests;
