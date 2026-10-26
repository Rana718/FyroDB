use super::small_str::SmallStr;
use std::collections::HashMap;

const CHUNK_MAX: usize = 64;
const INDEX_ON: usize = 128;
const INDEX_OFF: usize = 64;

type Index = HashMap<SmallStr, f64, foldhash::fast::RandomState>;

#[derive(Clone)]
pub struct ZSetData {
   chunks: Vec<Vec<ZEntry>>,
   index: Option<Box<Index>>,
   len: usize,
}

#[derive(Clone)]
pub struct ZEntry {
   pub score: f64,
   pub member: SmallStr,
}

#[inline(always)]
fn before(e: &ZEntry, score: f64, member: &str) -> bool {
   e.score < score || (e.score == score && e.member.as_str() < member)
}

#[derive(Clone)]
pub struct ZRange<'a> {
   chunks: &'a [Vec<ZEntry>],
   ci: usize,
   ei: usize,
   cj: usize,
   ej: usize,
   left: usize,
}

impl<'a> Iterator for ZRange<'a> {
   type Item = &'a ZEntry;

   #[inline]
   fn next(&mut self) -> Option<&'a ZEntry> {
      if self.left == 0 {
         return None;
      }
      while self.ei >= self.chunks[self.ci].len() {
         self.ci += 1;
         self.ei = 0;
      }
      let e = &self.chunks[self.ci][self.ei];
      self.ei += 1;
      self.left -= 1;
      Some(e)
   }

   #[inline]
   fn size_hint(&self) -> (usize, Option<usize>) {
      (self.left, Some(self.left))
   }
}

impl DoubleEndedIterator for ZRange<'_> {
   #[inline]
   fn next_back(&mut self) -> Option<Self::Item> {
      if self.left == 0 {
         return None;
      }
      while self.ej == 0 {
         self.cj -= 1;
         self.ej = self.chunks[self.cj].len();
      }
      self.ej -= 1;
      self.left -= 1;
      Some(&self.chunks[self.cj][self.ej])
   }
}

impl ExactSizeIterator for ZRange<'_> {}

impl ZSetData {
   pub fn new() -> Self {
      Self {
         chunks: Vec::new(),
         index: None,
         len: 0,
      }
   }

   pub fn with_capacity(cap: usize) -> Self {
      let mut chunks = Vec::with_capacity(cap / CHUNK_MAX + 1);
      if cap > 0 {
         chunks.push(Vec::with_capacity(cap.clamp(4, CHUNK_MAX)));
      }
      Self {
         chunks,
         index: None,
         len: 0,
      }
   }

   pub fn reserve(&mut self, additional: usize) {
      if let Some(ix) = &mut self.index {
         ix.reserve(additional);
      }
   }

   #[inline]
   pub fn len(&self) -> usize {
      self.len
   }

   #[inline]
   pub fn is_empty(&self) -> bool {
      self.len == 0
   }

   #[inline]
   fn bound(&self, pred: impl Fn(&ZEntry) -> bool) -> (usize, usize) {
      let ci = self.chunks.partition_point(|c| c.last().is_some_and(&pred));
      if ci == self.chunks.len() {
         return (ci, 0);
      }
      (ci, self.chunks[ci].partition_point(&pred))
   }

   #[inline]
   fn rank_of(&self, ci: usize, ei: usize) -> usize {
      self.chunks[..ci].iter().map(Vec::len).sum::<usize>() + ei
   }

   fn locate_rank(&self, mut r: usize) -> (usize, usize) {
      for (ci, c) in self.chunks.iter().enumerate() {
         if r < c.len() {
            return (ci, r);
         }
         r -= c.len();
      }
      (self.chunks.len(), 0)
   }

   fn span(&self, start: usize, end: usize) -> ZRange<'_> {
      let end = end.min(self.len);
      let start = start.min(end);
      let (ci, ei) = self.locate_rank(start);
      let (cj, ej) = self.locate_rank(end);
      ZRange {
         chunks: &self.chunks,
         ci,
         ei,
         cj,
         ej,
         left: end - start,
      }
   }

   fn span_pos(&self, a: (usize, usize), b: (usize, usize)) -> ZRange<'_> {
      let ra = self.rank_of(a.0, a.1);
      let rb = self.rank_of(b.0, b.1);
      ZRange {
         chunks: &self.chunks,
         ci: a.0,
         ei: a.1,
         cj: b.0,
         ej: b.1,
         left: rb.saturating_sub(ra),
      }
   }

   #[inline]
   fn score_of(&self, member: &str) -> Option<f64> {
      match &self.index {
         Some(ix) => ix.get(member).copied(),
         None => self
            .chunks
            .iter()
            .flatten()
            .find(|e| e.member.as_str() == member)
            .map(|e| e.score),
      }
   }

   fn locate_member(&self, member: &str) -> Option<(usize, usize)> {
      if self.index.is_none() {
         for (ci, c) in self.chunks.iter().enumerate() {
            if let Some(ei) = c.iter().position(|e| e.member.as_str() == member) {
               return Some((ci, ei));
            }
         }
         return None;
      }
      let score = self.score_of(member)?;
      let (ci, ei) = self.bound(|e| before(e, score, member));
      let e = self.chunks.get(ci)?.get(ei)?;
      (e.member.as_str() == member).then_some((ci, ei))
   }

   fn sync_index(&mut self) {
      if self.index.is_none() && self.len > INDEX_ON {
         let mut ix = Index::with_capacity_and_hasher(self.len, Default::default());
         for e in self.chunks.iter().flatten() {
            ix.insert(e.member.clone(), e.score);
         }
         self.index = Some(Box::new(ix));
      } else if self.index.is_some() && self.len < INDEX_OFF {
         self.index = None;
      }
   }

   fn insert_new(&mut self, score: f64, member: &str) {
      let entry = ZEntry {
         score,
         member: SmallStr::new(member),
      };
      if let Some(ix) = &mut self.index {
         ix.insert(entry.member.clone(), score);
      }
      self.len += 1;
      self.place(entry);
      if self.index.is_none() && self.len > INDEX_ON {
         self.sync_index();
      }
   }

   fn place(&mut self, entry: ZEntry) {
      let (score, member) = (entry.score, entry.member.as_str());
      let Some(last) = self.chunks.last_mut() else {
         let mut c = Vec::with_capacity(4);
         c.push(entry);
         self.chunks.push(c);
         return;
      };
      let ci = if last.is_empty() || last.last().is_some_and(|e| before(e, score, member)) {
         last.push(entry);
         self.chunks.len() - 1
      } else {
         let (mut ci, mut ei) = self.bound(|e| before(e, score, member));
         if ci == self.chunks.len() {
            ci -= 1;
            ei = self.chunks[ci].len();
         }
         self.chunks[ci].insert(ei, entry);
         ci
      };
      if self.chunks[ci].len() > CHUNK_MAX {
         if ci + 1 == self.chunks.len() {
            let mut new_chunk = Vec::with_capacity(CHUNK_MAX / 2);
            new_chunk.push(self.chunks[ci].pop().unwrap());
            self.chunks.push(new_chunk);
         } else {
            let tail = {
               let c = &mut self.chunks[ci];
               c.split_off(c.len() / 2)
            };
            self.chunks.insert(ci + 1, tail);
         }
      }
   }

   fn remove_at(&mut self, ci: usize, ei: usize) -> ZEntry {
      let e = self.chunks[ci].remove(ei);
      self.len -= 1;
      if let Some(ix) = &mut self.index {
         ix.remove(e.member.as_str());
      }
      if self.chunks[ci].is_empty() {
         self.chunks.remove(ci);
      } else if ci + 1 < self.chunks.len()
         && self.chunks[ci].len() + self.chunks[ci + 1].len() <= CHUNK_MAX / 2
      {
         let next = self.chunks.remove(ci + 1);
         self.chunks[ci].extend(next);
      }
      self.sync_index();
      e
   }

   pub fn insert(&mut self, score: f64, member: &str) -> bool {
      if let Some(ix) = self.index.as_deref_mut() {
         if let Some(&old) = ix.get(member) {
            if (old - score).abs() > f64::EPSILON {
               self.relocate(member, score);
            }
            return false;
         }
         let m = SmallStr::new(member);
         ix.insert(m.clone(), score);
         self.len += 1;
         self.place(ZEntry { score, member: m });
         return true;
      }
      match self.score_of(member) {
         Some(old) => {
            if (old - score).abs() > f64::EPSILON {
               self.relocate(member, score);
            }
            false
         }
         None => {
            self.insert_new(score, member);
            true
         }
      }
   }

   fn relocate(&mut self, member: &str, score: f64) {
      let Some((ci, ei)) = self.locate_member(member) else {
         return;
      };
      let c = &self.chunks[ci];
      let prev_ok = if ei > 0 {
         before(&c[ei - 1], score, member)
      } else {
         ci == 0
            || self.chunks[ci - 1]
               .last()
               .is_some_and(|p| before(p, score, member))
      };
      let next_ok = if ei + 1 < c.len() {
         !before(&c[ei + 1], score, member)
      } else {
         ci + 1 >= self.chunks.len()
            || self.chunks[ci + 1]
               .first()
               .is_some_and(|n| !before(n, score, member))
      };
      if prev_ok && next_ok {
         self.chunks[ci][ei].score = score;
         if let Some(ix) = &mut self.index
            && let Some(s) = ix.get_mut(member)
         {
            *s = score;
         }
         return;
      }
      let old = self.remove_at(ci, ei);
      self.insert_new(score, old.member.as_str());
   }

   pub fn remove(&mut self, member: &str) -> Option<f64> {
      let (ci, ei) = self.locate_member(member)?;
      Some(self.remove_at(ci, ei).score)
   }

   #[inline]
   pub fn get_score(&self, member: &str) -> Option<f64> {
      self.score_of(member)
   }

   pub fn rank(&self, member: &str) -> Option<usize> {
      let (ci, ei) = self.locate_member(member)?;
      Some(self.rank_of(ci, ei))
   }

   pub fn rev_rank(&self, member: &str) -> Option<usize> {
      self.rank(member).map(|r| self.len - 1 - r)
   }

   pub fn get(&self, rank: usize) -> Option<&ZEntry> {
      let (ci, ei) = self.locate_rank(rank);
      self.chunks.get(ci)?.get(ei)
   }

   pub fn range_by_rank(&self, start: usize, end: usize) -> ZRange<'_> {
      self.span(start, end)
   }

   pub fn range_by_score(&self, min: f64, max: f64) -> ZRange<'_> {
      let a = self.bound(|e| e.score < min);
      let b = self.bound(|e| e.score <= max);
      self.span_pos(a, b)
   }

   pub fn rev_range_by_score(&self, min: f64, max: f64) -> Vec<&ZEntry> {
      self.range_by_score(min, max).rev().collect()
   }

   pub fn count_in_score_range(&self, min: f64, max: f64) -> usize {
      self.range_by_score(min, max).len()
   }

   pub fn pop_min(&mut self) -> Option<ZEntry> {
      if self.len == 0 {
         return None;
      }
      Some(self.remove_at(0, 0))
   }

   pub fn pop_max(&mut self) -> Option<ZEntry> {
      let ci = self.chunks.len().checked_sub(1)?;
      let ei = self.chunks[ci].len() - 1;
      Some(self.remove_at(ci, ei))
   }

   pub fn iter(&self) -> ZRange<'_> {
      self.span(0, self.len)
   }

   pub fn iter_rev(&self) -> std::iter::Rev<ZRange<'_>> {
      self.iter().rev()
   }

   pub fn contains(&self, member: &str) -> bool {
      self.score_of(member).is_some()
   }

   pub fn members(&self) -> impl Iterator<Item = &SmallStr> {
      self.iter().map(|e| &e.member)
   }

   pub fn random_members(&self, count: usize, allow_dup: bool) -> Vec<&ZEntry> {
      use std::collections::HashSet as StdSet;
      if self.len == 0 {
         return vec![];
      }
      let n = self.len;
      let mut rng = n.wrapping_mul(6364136223846793005).wrapping_add(1);
      let mut next = || -> usize {
         rng ^= rng << 13;
         rng ^= rng >> 7;
         rng ^= rng << 17;
         rng % n
      };
      if allow_dup {
         (0..count).filter_map(|_| self.get(next())).collect()
      } else if count >= n {
         self.iter().collect()
      } else {
         let mut seen = StdSet::with_capacity(count);
         let mut result = Vec::with_capacity(count);
         while result.len() < count {
            let idx = next();
            if seen.insert(idx)
               && let Some(e) = self.get(idx)
            {
               result.push(e);
            }
         }
         result
      }
   }

   #[inline]
   fn in_lex(e: &ZEntry, min: &str, max: &str, min_exclusive: bool, max_exclusive: bool) -> bool {
      let m = e.member.as_str();
      let above_min = if min_exclusive { m > min } else { m >= min };
      let below_max = if max == "+" {
         true
      } else if max_exclusive {
         m < max
      } else {
         m <= max
      };
      above_min && below_max
   }

   pub fn lex_range(
      &self,
      min: &str,
      max: &str,
      min_exclusive: bool,
      max_exclusive: bool,
   ) -> Vec<&ZEntry> {
      self
         .iter()
         .filter(|e| Self::in_lex(e, min, max, min_exclusive, max_exclusive))
         .collect()
   }

   pub fn count_lex_range(
      &self,
      min: &str,
      max: &str,
      min_exclusive: bool,
      max_exclusive: bool,
   ) -> usize {
      self
         .iter()
         .filter(|e| Self::in_lex(e, min, max, min_exclusive, max_exclusive))
         .count()
   }

   pub fn remove_range_by_rank(&mut self, start: usize, end: usize) -> usize {
      let end = end.min(self.len);
      if start >= end {
         return 0;
      }
      let mut r = 0;
      let mut removed = 0;
      for c in self.chunks.iter_mut() {
         if r >= end {
            break;
         }
         let clen = c.len();
         let lo = start.saturating_sub(r).min(clen);
         let hi = (end - r).min(clen);
         if lo < hi {
            for e in c.drain(lo..hi) {
               if let Some(ix) = &mut self.index {
                  ix.remove(e.member.as_str());
               }
            }
            removed += hi - lo;
         }
         r += clen;
      }
      self.finish_bulk_remove(removed);
      removed
   }

   pub fn remove_range_by_score(&mut self, min: f64, max: f64) -> usize {
      let a = self.bound(|e| e.score < min);
      let b = self.bound(|e| e.score <= max);
      let ra = self.rank_of(a.0, a.1);
      let rb = self.rank_of(b.0, b.1);
      self.remove_range_by_rank(ra, rb)
   }

   pub fn remove_lex_range(
      &mut self,
      min: &str,
      max: &str,
      min_exclusive: bool,
      max_exclusive: bool,
   ) -> usize {
      let mut removed = 0;
      let index = &mut self.index;
      for c in self.chunks.iter_mut() {
         c.retain(|e| {
            let hit = Self::in_lex(e, min, max, min_exclusive, max_exclusive);
            if hit {
               removed += 1;
               if let Some(ix) = index {
                  ix.remove(e.member.as_str());
               }
            }
            !hit
         });
      }
      self.finish_bulk_remove(removed);
      removed
   }

   fn finish_bulk_remove(&mut self, removed: usize) {
      if removed == 0 {
         return;
      }
      self.len -= removed;
      self.chunks.retain(|c| !c.is_empty());
      let mut i = 0;
      while i + 1 < self.chunks.len() {
         if self.chunks[i].len() + self.chunks[i + 1].len() <= CHUNK_MAX / 2 {
            let next = self.chunks.remove(i + 1);
            self.chunks[i].extend(next);
         } else {
            i += 1;
         }
      }
      for c in self.chunks.iter_mut() {
         if c.capacity() > c.len().saturating_mul(2).max(16) {
            c.shrink_to_fit();
         }
      }
      self.sync_index();
   }

   pub fn incr(&mut self, member: &str, increment: f64) -> f64 {
      match self.score_of(member) {
         Some(old) => {
            let new_score = old + increment;
            self.relocate(member, new_score);
            new_score
         }
         None => {
            self.insert_new(increment, member);
            increment
         }
      }
   }

   pub fn shrink_to_fit(&mut self) {
      for c in self.chunks.iter_mut() {
         c.shrink_to_fit();
      }
      self.chunks.shrink_to_fit();
      if let Some(ix) = &mut self.index {
         ix.shrink_to_fit();
      }
   }
}

impl Default for ZSetData {
   fn default() -> Self {
      Self::new()
   }
}

#[cfg(test)]
mod tests {
   use super::*;
   use std::collections::BTreeMap;

   fn ordered(z: &ZSetData) -> Vec<(f64, String)> {
      z.iter().map(|e| (e.score, e.member.to_string())).collect()
   }

   fn check(z: &ZSetData, model: &BTreeMap<String, f64>) {
      let mut want: Vec<(f64, String)> = model.iter().map(|(m, s)| (*s, m.clone())).collect();
      want.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
      assert_eq!(z.len(), want.len());
      assert_eq!(ordered(z), want);
      let rev: Vec<_> = z.iter_rev().map(|e| e.member.to_string()).collect();
      let mut fwd: Vec<_> = want.iter().map(|w| w.1.clone()).collect();
      fwd.reverse();
      assert_eq!(rev, fwd);
      for (r, (s, m)) in want.iter().enumerate().step_by(7) {
         assert_eq!(z.get_score(m), Some(*s));
         assert_eq!(z.rank(m), Some(r));
         assert_eq!(z.get(r).map(|e| e.member.as_str()), Some(m.as_str()));
      }
   }

   #[test]
   fn matches_a_reference_model_under_random_ops() {
      let mut z = ZSetData::new();
      let mut model = BTreeMap::new();
      let mut rng = 0x9e37_79b9_7f4a_7c15u64;
      let mut next = |n: u64| {
         rng ^= rng << 13;
         rng ^= rng >> 7;
         rng ^= rng << 17;
         rng % n
      };
      for step in 0..60_000 {
         let m = format!("m{}", next(3000));
         match next(10) {
            0..=4 => {
               let s = next(500) as f64;
               let fresh = z.insert(s, &m);
               assert_eq!(fresh, !model.contains_key(&m));
               model.insert(m, s);
            }
            5 | 6 => assert_eq!(z.remove(&m), model.remove(&m)),
            7 => {
               let inc = next(20) as f64 - 10.0;
               let got = z.incr(&m, inc);
               let e = model.entry(m).or_insert(0.0);
               *e += inc;
               assert_eq!(got, *e);
            }
            8 => {
               let lo = next(500) as f64;
               let hi = lo + next(20) as f64;
               let want = model.values().filter(|s| **s >= lo && **s <= hi).count();
               assert_eq!(z.count_in_score_range(lo, hi), want);
               if step % 5 == 0 {
                  assert_eq!(z.remove_range_by_score(lo, hi), want);
                  model.retain(|_, s| *s < lo || *s > hi);
               }
            }
            _ => {
               if step % 3 == 0 && !model.is_empty() {
                  let a = next(model.len() as u64) as usize;
                  let b = (a + next(10) as usize).min(model.len());
                  let mut want: Vec<(f64, String)> =
                     model.iter().map(|(m, s)| (*s, m.clone())).collect();
                  want.sort_by(|x, y| x.0.total_cmp(&y.0).then_with(|| x.1.cmp(&y.1)));
                  let got: Vec<_> = z
                     .range_by_rank(a, b)
                     .map(|e| e.member.to_string())
                     .collect();
                  let exp: Vec<_> = want[a..b].iter().map(|w| w.1.clone()).collect();
                  assert_eq!(got, exp);
                  assert_eq!(z.remove_range_by_rank(a, b), b - a);
                  for w in &want[a..b] {
                     model.remove(&w.1);
                  }
               }
            }
         }
         if step % 997 == 0 {
            check(&z, &model);
         }
      }
      check(&z, &model);
      while let Some(e) = z.pop_min() {
         assert!(model.remove(e.member.as_str()).is_some());
      }
      assert!(model.is_empty());
   }

   #[test]
   fn re_adding_members_does_not_duplicate_them() {
      let mut zset = ZSetData::new();
      for i in 0..2000 {
         zset.insert(i as f64, &format!("m{i}"));
      }
      for i in 0..2000 {
         assert!(!zset.insert(i as f64, &format!("m{i}")), "m{i} counted new");
      }
      assert_eq!(zset.len(), 2000);
      for i in 0..2000 {
         assert_eq!(zset.get_score(&format!("m{i}")), Some(i as f64));
      }
   }

   #[test]
   fn changed_score_re_add_moves_the_member() {
      let mut zset = ZSetData::new();
      for i in 0..1000 {
         zset.insert(i as f64, &format!("m{i}"));
      }
      for i in 0..1000 {
         assert!(!zset.insert(i as f64 + 2000.0, &format!("m{i}")));
         assert_eq!(zset.get_score(&format!("m{i}")), Some(i as f64 + 2000.0));
      }
      assert_eq!(zset.len(), 1000);
      let scores: Vec<f64> = zset.iter().map(|e| e.score).collect();
      assert!(scores.windows(2).all(|w| w[0] <= w[1]));
   }

   #[test]
   fn member_ops_stay_sublinear_on_large_sets() {
      const N: usize = 200_000;
      let mut zset = ZSetData::new();
      for i in 0..N {
         zset.insert(((i * 7919) % N) as f64, &format!("m{i}"));
      }
      let start = std::time::Instant::now();
      for i in (0..N).step_by(3) {
         let m = format!("m{i}");
         assert!(zset.rank(&m).is_some());
         zset.incr(&m, 0.5);
         zset.remove(&m);
         zset.insert(1.0, &m);
      }
      let elapsed = start.elapsed();
      assert!(elapsed.as_millis() < 3000, "{elapsed:?}");
      assert_eq!(zset.len(), N);
   }
}
