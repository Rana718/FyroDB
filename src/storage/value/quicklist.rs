use std::collections::VecDeque;

const CHUNK_ENTRIES: usize = 128;
const CHUNK_BYTES: usize = 4096;
const FRONT_SLOTS: usize = 16;

#[derive(Clone)]
pub struct Chunk {
    data: Vec<u8>,
    offs: Vec<u32>,
    head: usize,
}

#[inline(always)]
fn as_str(b: &[u8]) -> &str {
    unsafe { std::str::from_utf8_unchecked(b) }
}

impl Chunk {
    fn new() -> Self {
        Self {
            data: Vec::new(),
            offs: vec![0],
            head: 0,
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.offs.len() - 1 - self.head
    }

    #[inline]
    fn live_bytes(&self) -> usize {
        self.data.len() - self.offs[self.head] as usize
    }

    #[inline]
    fn get(&self, j: usize) -> &str {
        let i = self.head + j;
        as_str(&self.data[self.offs[i] as usize..self.offs[i + 1] as usize])
    }

    fn reset(&mut self) {
        self.data.clear();
        self.offs.clear();
        self.offs.push(0);
        self.head = 0;
        if self.data.capacity() > CHUNK_BYTES * 2 {
            self.data.shrink_to(CHUNK_BYTES);
        }
    }

    fn seal(&mut self) {
        if self.head > 0 {
            self.rebuild(0, 0);
        }
        self.data.shrink_to_fit();
        self.offs.shrink_to_fit();
    }

    #[inline]
    fn full_back(&self) -> bool {
        self.len() >= CHUNK_ENTRIES || self.data.len() >= CHUNK_BYTES
    }

    #[inline]
    fn full_front(&self) -> bool {
        self.len() >= CHUNK_ENTRIES || self.live_bytes() >= CHUNK_BYTES
    }

    #[inline]
    fn push_back(&mut self, s: &str) {
        self.data.extend_from_slice(s.as_bytes());
        self.offs.push(self.data.len() as u32);
    }

    fn push_front(&mut self, s: &str) {
        let n = s.len();
        if self.head == 0 || (self.offs[self.head] as usize) < n {
            self.rebuild(FRONT_SLOTS, n.max(self.live_bytes() / 2).max(64));
        }
        let end = self.offs[self.head] as usize;
        let start = end - n;
        self.data[start..end].copy_from_slice(s.as_bytes());
        self.head -= 1;
        self.offs[self.head] = start as u32;
    }

    fn rebuild(&mut self, slots: usize, gap: usize) {
        let base = self.offs[self.head] as usize;
        let live = self.len();
        let mut data = Vec::with_capacity(gap + self.data.len() - base);
        data.resize(gap, 0);
        data.extend_from_slice(&self.data[base..]);
        let mut offs = Vec::with_capacity(slots + live + 1);
        offs.resize(slots, gap as u32);
        for &o in &self.offs[self.head..] {
            offs.push((o as usize - base + gap) as u32);
        }
        self.data = data;
        self.offs = offs;
        self.head = slots;
    }

    fn pop_back_with<R>(&mut self, f: impl FnOnce(&str) -> R) -> R {
        let last = self.offs.len() - 2;
        let start = self.offs[last] as usize;
        let r = f(as_str(&self.data[start..]));
        self.data.truncate(start);
        self.offs.pop();
        if self.len() == 0 {
            self.reset();
        }
        r
    }

    fn pop_front_with<R>(&mut self, f: impl FnOnce(&str) -> R) -> R {
        let r = f(self.get(0));
        self.head += 1;
        if self.len() == 0 {
            self.reset();
        }
        r
    }

    fn from_iter<'a>(items: impl Iterator<Item = &'a str>) -> Self {
        let mut c = Self::new();
        for s in items {
            c.push_back(s);
        }
        c
    }

    fn iter(&self) -> ChunkIter<'_> {
        ChunkIter {
            c: self,
            i: self.head,
            j: self.offs.len() - 1,
        }
    }

    fn replace_with<'a>(&mut self, items: impl Iterator<Item = &'a str>) {
        *self = Self::from_iter(items);
    }
}

#[derive(Clone)]
pub struct ChunkIter<'a> {
    c: &'a Chunk,
    i: usize,
    j: usize,
}

impl<'a> Iterator for ChunkIter<'a> {
    type Item = &'a str;

    #[inline]
    fn next(&mut self) -> Option<&'a str> {
        if self.i >= self.j {
            return None;
        }
        let s = &self.c.data[self.c.offs[self.i] as usize..self.c.offs[self.i + 1] as usize];
        self.i += 1;
        Some(as_str(s))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.j - self.i, Some(self.j - self.i))
    }
}

impl DoubleEndedIterator for ChunkIter<'_> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.i >= self.j {
            return None;
        }
        self.j -= 1;
        let s = &self.c.data[self.c.offs[self.j] as usize..self.c.offs[self.j + 1] as usize];
        Some(as_str(s))
    }
}

#[derive(Clone, Default)]
pub struct QuickList {
    chunks: VecDeque<Chunk>,
    len: usize,
}

pub type Iter<'a> = std::iter::FlatMap<
    std::collections::vec_deque::Iter<'a, Chunk>,
    ChunkIter<'a>,
    fn(&'a Chunk) -> ChunkIter<'a>,
>;

impl QuickList {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_strs<'a>(items: impl IntoIterator<Item = &'a str>) -> Self {
        let mut q = Self::new();
        for s in items {
            q.push_back(s);
        }
        q
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn memory_bytes(&self) -> usize {
        self.chunks
            .iter()
            .map(|c| c.data.capacity() + c.offs.capacity() * 4 + 64)
            .sum()
    }

    pub fn push_back(&mut self, s: &str) {
        match self.chunks.back_mut() {
            Some(c) if !c.full_back() => {}
            Some(c) => {
                c.seal();
                self.chunks.push_back(Chunk::new());
            }
            None => self.chunks.push_back(Chunk::new()),
        }
        self.chunks.back_mut().expect("chunk").push_back(s);
        self.len += 1;
    }

    pub fn push_front(&mut self, s: &str) {
        match self.chunks.front_mut() {
            Some(c) if !c.full_front() => {}
            Some(c) => {
                c.seal();
                self.chunks.push_front(Chunk::new());
            }
            None => self.chunks.push_front(Chunk::new()),
        }
        self.chunks.front_mut().expect("chunk").push_front(s);
        self.len += 1;
    }

    pub fn pop_front_with<R>(&mut self, f: impl FnOnce(&str) -> R) -> Option<R> {
        let c = self.chunks.front_mut()?;
        let r = c.pop_front_with(f);
        self.len -= 1;
        if c.len() == 0 {
            self.chunks.pop_front();
        }
        Some(r)
    }

    pub fn pop_back_with<R>(&mut self, f: impl FnOnce(&str) -> R) -> Option<R> {
        let c = self.chunks.back_mut()?;
        let r = c.pop_back_with(f);
        self.len -= 1;
        if c.len() == 0 {
            self.chunks.pop_back();
        }
        Some(r)
    }

    pub fn pop_front(&mut self) -> Option<String> {
        self.pop_front_with(str::to_string)
    }

    pub fn pop_back(&mut self) -> Option<String> {
        self.pop_back_with(str::to_string)
    }

    fn locate(&self, idx: usize) -> Option<(usize, usize)> {
        if idx >= self.len {
            return None;
        }
        if idx < self.len / 2 {
            let mut r = idx;
            for (ci, c) in self.chunks.iter().enumerate() {
                if r < c.len() {
                    return Some((ci, r));
                }
                r -= c.len();
            }
        } else {
            let mut r = self.len - 1 - idx;
            for (ci, c) in self.chunks.iter().enumerate().rev() {
                if r < c.len() {
                    return Some((ci, c.len() - 1 - r));
                }
                r -= c.len();
            }
        }
        None
    }

    pub fn get(&self, idx: usize) -> Option<&str> {
        let (ci, j) = self.locate(idx)?;
        Some(self.chunks[ci].get(j))
    }

    pub fn iter(&self) -> Iter<'_> {
        self.chunks.iter().flat_map(Chunk::iter as fn(&Chunk) -> ChunkIter<'_>)
    }

    pub fn range(&self, start: usize, count: usize) -> impl Iterator<Item = &str> {
        let (ci, j) = self.locate(start).unwrap_or((self.chunks.len(), 0));
        self.chunks
            .range(ci.min(self.chunks.len())..)
            .enumerate()
            .flat_map(move |(k, c)| c.iter().skip(if k == 0 { j } else { 0 }))
            .take(count)
    }

    fn rewrite_chunk(&mut self, ci: usize, items: Vec<String>) {
        let before = self.chunks[ci].len();
        self.len = self.len - before + items.len();
        if items.is_empty() {
            self.chunks.remove(ci);
            return;
        }
        if items.len() <= CHUNK_ENTRIES && items.iter().map(String::len).sum::<usize>() <= CHUNK_BYTES {
            self.chunks[ci].replace_with(items.iter().map(String::as_str));
            return;
        }
        let mid = items.len() / 2;
        self.chunks[ci].replace_with(items[..mid].iter().map(String::as_str));
        self.chunks
            .insert(ci + 1, Chunk::from_iter(items[mid..].iter().map(String::as_str)));
    }

    pub fn set(&mut self, idx: usize, value: &str) -> bool {
        let Some((ci, j)) = self.locate(idx) else {
            return false;
        };
        let mut items: Vec<String> = self.chunks[ci].iter().map(str::to_string).collect();
        items[j] = value.to_string();
        self.rewrite_chunk(ci, items);
        true
    }

    pub fn insert(&mut self, idx: usize, value: &str) {
        if idx == 0 {
            return self.push_front(value);
        }
        if idx >= self.len {
            return self.push_back(value);
        }
        let (ci, j) = self.locate(idx).expect("in range");
        let mut items: Vec<String> = self.chunks[ci].iter().map(str::to_string).collect();
        items.insert(j, value.to_string());
        self.rewrite_chunk(ci, items);
    }

    pub fn position(&self, value: &str) -> Option<usize> {
        self.iter().position(|v| v == value)
    }

    pub fn remove_matching(&mut self, value: &str, limit: usize, from_back: bool) -> usize {
        let mut removed = 0;
        let order: Vec<usize> = if from_back {
            (0..self.chunks.len()).rev().collect()
        } else {
            (0..self.chunks.len()).collect()
        };
        let mut dead = Vec::new();
        for ci in order {
            if removed >= limit {
                break;
            }
            let c = &self.chunks[ci];
            if !c.iter().any(|v| v == value) {
                continue;
            }
            let mut items: Vec<&str> = c.iter().collect();
            let mut k = 0;
            if from_back {
                let mut i = items.len();
                while i > 0 && removed < limit {
                    i -= 1;
                    if items[i] == value {
                        items.remove(i);
                        removed += 1;
                        k += 1;
                    }
                }
            } else {
                let mut i = 0;
                while i < items.len() && removed < limit {
                    if items[i] == value {
                        items.remove(i);
                        removed += 1;
                        k += 1;
                    } else {
                        i += 1;
                    }
                }
            }
            let owned: Vec<String> = items.into_iter().map(str::to_string).collect();
            self.len -= k;
            if owned.is_empty() {
                dead.push(ci);
            } else {
                self.chunks[ci].replace_with(owned.iter().map(String::as_str));
            }
        }
        dead.sort_unstable();
        for ci in dead.into_iter().rev() {
            self.chunks.remove(ci);
        }
        removed
    }

    pub fn trim(&mut self, start: usize, keep: usize) {
        let mut skip = start;
        while skip > 0 {
            let Some(c) = self.chunks.front_mut() else {
                break;
            };
            if c.len() <= skip {
                skip -= c.len();
                self.len -= c.len();
                self.chunks.pop_front();
            } else {
                for _ in 0..skip {
                    c.pop_front_with(|_| ());
                }
                self.len -= skip;
                skip = 0;
            }
        }
        while self.len > keep {
            let excess = self.len - keep;
            let Some(c) = self.chunks.back_mut() else {
                break;
            };
            if c.len() <= excess {
                self.len -= c.len();
                self.chunks.pop_back();
            } else {
                for _ in 0..excess {
                    c.pop_back_with(|_| ());
                }
                self.len -= excess;
            }
        }
        self.shrink_to_fit();
    }

    pub fn clear(&mut self) {
        self.chunks.clear();
        self.chunks.shrink_to_fit();
        self.len = 0;
    }

    pub fn shrink_to_fit(&mut self) {
        if self.chunks.capacity() > self.chunks.len().saturating_mul(2).max(8) {
            self.chunks.shrink_to_fit();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(q: &QuickList, model: &VecDeque<String>) {
        assert_eq!(q.len(), model.len());
        let got: Vec<&str> = q.iter().collect();
        let want: Vec<&str> = model.iter().map(String::as_str).collect();
        assert_eq!(got, want);
        let rev: Vec<&str> = q.iter().rev().collect();
        let mut w2 = want.clone();
        w2.reverse();
        assert_eq!(rev, w2);
        for i in (0..model.len()).step_by(13) {
            assert_eq!(q.get(i), Some(model[i].as_str()));
        }
        if !model.is_empty() {
            let s = model.len() / 3;
            let n = 50.min(model.len() - s);
            let r: Vec<&str> = q.range(s, n).collect();
            assert_eq!(r, want[s..s + n].to_vec());
        }
    }

    #[test]
    fn matches_vecdeque_under_random_ops() {
        let mut q = QuickList::new();
        let mut m: VecDeque<String> = VecDeque::new();
        let mut rng = 0x2545_f491_4f6c_dd1du64;
        let mut next = |n: u64| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng % n
        };
        for step in 0..80_000u64 {
            let v = format!("v{}{}", next(50), "x".repeat(next(40) as usize));
            match next(12) {
                0..=2 => {
                    q.push_back(&v);
                    m.push_back(v);
                }
                3..=5 => {
                    q.push_front(&v);
                    m.push_front(v);
                }
                6 => assert_eq!(q.pop_front(), m.pop_front()),
                7 => assert_eq!(q.pop_back(), m.pop_back()),
                8 if !m.is_empty() => {
                    let i = next(m.len() as u64) as usize;
                    assert!(q.set(i, &v));
                    m[i] = v;
                }
                9 => {
                    let i = next(m.len() as u64 + 1) as usize;
                    q.insert(i, &v);
                    m.insert(i, v);
                }
                10 if step % 7 == 0 => {
                    let target = format!("v{}", next(50));
                    let limit = next(3) as usize + 1;
                    let back = next(2) == 0;
                    let got = q.remove_matching(&target, limit, back);
                    let mut removed = 0;
                    if back {
                        let mut i = m.len();
                        while i > 0 && removed < limit {
                            i -= 1;
                            if m[i] == target {
                                m.remove(i);
                                removed += 1;
                            }
                        }
                    } else {
                        let mut i = 0;
                        while i < m.len() && removed < limit {
                            if m[i] == target {
                                m.remove(i);
                                removed += 1;
                            } else {
                                i += 1;
                            }
                        }
                    }
                    assert_eq!(got, removed);
                }
                11 if step % 50 == 0 && m.len() > 10 => {
                    let s = next(5) as usize;
                    let keep = (m.len() - s).saturating_sub(next(5) as usize);
                    q.trim(s, keep);
                    m.drain(..s);
                    m.truncate(keep);
                }
                _ => {}
            }
            if step % 4001 == 0 {
                check(&q, &m);
            }
        }
        check(&q, &m);
    }

    #[test]
    fn packs_small_elements_densely() {
        let mut q = QuickList::new();
        for i in 0..100_000 {
            q.push_back(&format!("{i:08}"));
        }
        assert!(q.memory_bytes() < 100_000 * 16, "{}", q.memory_bytes());
        for i in 0..100_000 {
            assert_eq!(q.pop_front().unwrap(), format!("{i:08}"));
        }
        assert!(q.is_empty());
        for i in 0..10_000 {
            q.push_front(&format!("{i}"));
        }
        assert_eq!(q.get(0), Some("9999"));
        assert_eq!(q.get(9999), Some("0"));
        assert!(q.memory_bytes() < 10_000 * 24);
    }
}
