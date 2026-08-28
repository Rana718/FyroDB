use super::small_str::SmallStr;

#[derive(Clone)]
pub struct ZSetData {
    entries: Vec<ZEntry>,
    bloom: Vec<u64>,
}

#[derive(Clone)]
pub struct ZEntry {
    pub score: f64,
    pub member: SmallStr,
}

#[inline]
fn member_hash(member: &str) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for &b in member.as_bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Bits probed per member.
///
/// The filter keeps 8–16 bits per entry, where the optimal probe count is
/// `m/n * ln2` ≈ 6–11. Two probes left the false-positive rate around 3–5%, and
/// every false positive costs a full linear scan of the member list — the
/// dominant cost of `ZADD` into a large sorted set. Five probes cut that rate by
/// roughly 4x for the same memory and a handful of extra bit tests.
const BLOOM_PROBES: u32 = 5;

/// Split one hash into the pair used for double hashing.
///
/// `h2` is forced odd so successive probes stride the whole bit space instead of
/// cycling through a subset.
#[inline(always)]
fn bloom_seeds(h: u64) -> (u64, u64) {
    (h, h.rotate_left(31) | 1)
}

#[inline(always)]
fn bloom_position(h1: u64, h2: u64, probe: u32, bits: usize) -> usize {
    // `bits` is always a power of two, so the mask is exact.
    (h1.wrapping_add((probe as u64).wrapping_mul(h2)) as usize) & (bits - 1)
}

impl ZSetData {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            bloom: vec![0],
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        let words = (cap.saturating_mul(8).next_power_of_two().max(64) / 64).max(1);
        Self {
            entries: Vec::with_capacity(cap),
            bloom: vec![0; words],
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[inline]
    fn bloom_maybe_contains(&self, h: u64) -> bool {
        let bits = self.bloom.len() * 64;
        let (h1, h2) = bloom_seeds(h);
        for probe in 0..BLOOM_PROBES {
            let index = bloom_position(h1, h2, probe, bits);
            if (self.bloom[index / 64] & (1u64 << (index % 64))) == 0 {
                return false;
            }
        }
        true
    }

    #[inline]
    fn bloom_set(&mut self, h: u64) {
        let bits = self.bloom.len() * 64;
        let (h1, h2) = bloom_seeds(h);
        for probe in 0..BLOOM_PROBES {
            let index = bloom_position(h1, h2, probe, bits);
            self.bloom[index / 64] |= 1u64 << (index % 64);
        }
    }

    fn bloom_grow_if_needed(&mut self) {
        if self.entries.len().saturating_mul(8) > self.bloom.len() * 64 {
            self.bloom_rebuild();
        }
    }

    fn bloom_rebuild(&mut self) {
        let words = (self
            .entries
            .len()
            .saturating_mul(8)
            .next_power_of_two()
            .max(64)
            / 64)
            .max(1);
        self.bloom.clear();
        self.bloom.resize(words, 0);
        let bits = self.bloom.len() * 64;
        for i in 0..self.entries.len() {
            let h = member_hash(self.entries[i].member.as_str());
            let (h1, h2) = bloom_seeds(h);
            for probe in 0..BLOOM_PROBES {
                let index = bloom_position(h1, h2, probe, bits);
                self.bloom[index / 64] |= 1u64 << (index % 64);
            }
        }
    }

    pub fn insert(&mut self, score: f64, member: &str) -> bool {
        let h = member_hash(member);
        // Fast check: if bloom says "definitely not here", skip linear scan
        if self.bloom_maybe_contains(h) {
            // Check last entry first (ascending pattern)
            if let Some(last) = self.entries.last()
                && last.member.as_str() == member
            {
                if (last.score - score).abs() > f64::EPSILON {
                    self.entries.pop();
                    let insert_pos = self.find_insert_pos(score, member);
                    self.entries.insert(
                        insert_pos,
                        ZEntry {
                            score,
                            member: SmallStr::new(member),
                        },
                    );
                }
                return false;
            }
            if let Some(pos) = self
                .entries
                .iter()
                .position(|e| e.member.as_str() == member)
            {
                let old_score = self.entries[pos].score;
                if (old_score - score).abs() > f64::EPSILON {
                    self.entries.remove(pos);
                    let insert_pos = self.find_insert_pos(score, member);
                    self.entries.insert(
                        insert_pos,
                        ZEntry {
                            score,
                            member: SmallStr::new(member),
                        },
                    );
                }
                return false;
            }
        }
        if self.entries.last().is_none_or(|last| {
            last.score < score || (last.score == score && last.member.as_str() <= member)
        }) {
            self.entries.push(ZEntry {
                score,
                member: SmallStr::new(member),
            });
        } else {
            let insert_pos = self.find_insert_pos(score, member);
            self.entries.insert(
                insert_pos,
                ZEntry {
                    score,
                    member: SmallStr::new(member),
                },
            );
        }
        self.bloom_set(h);
        self.bloom_grow_if_needed();
        true
    }

    pub fn remove(&mut self, member: &str) -> Option<f64> {
        let pos = self
            .entries
            .iter()
            .position(|e| e.member.as_str() == member)?;
        let score = self.entries[pos].score;
        self.entries.remove(pos);
        self.reclaim_capacity();
        Some(score)
    }

    #[inline]
    pub fn get_score(&self, member: &str) -> Option<f64> {
        self.entries
            .iter()
            .find(|e| e.member.as_str() == member)
            .map(|e| e.score)
    }

    pub fn rank(&self, member: &str) -> Option<usize> {
        self.entries
            .iter()
            .position(|e| e.member.as_str() == member)
    }

    pub fn rev_rank(&self, member: &str) -> Option<usize> {
        self.rank(member).map(|r| self.entries.len() - 1 - r)
    }

    pub fn range_by_rank(&self, start: usize, end: usize) -> &[ZEntry] {
        let end = end.min(self.entries.len());
        if start >= end {
            return &[];
        }
        &self.entries[start..end]
    }

    /// O(log n) binary search on sorted entries.
    pub fn range_by_score(&self, min: f64, max: f64) -> &[ZEntry] {
        let start = self.entries.partition_point(|e| e.score < min);
        let end = self.entries.partition_point(|e| e.score <= max);
        &self.entries[start..end]
    }

    /// O(log n) binary search + reverse iteration.
    pub fn rev_range_by_score(&self, min: f64, max: f64) -> Vec<&ZEntry> {
        let start = self.entries.partition_point(|e| e.score < min);
        let end = self.entries.partition_point(|e| e.score <= max);
        self.entries[start..end].iter().rev().collect()
    }

    /// O(log n) count.
    pub fn count_in_score_range(&self, min: f64, max: f64) -> usize {
        let start = self.entries.partition_point(|e| e.score < min);
        let end = self.entries.partition_point(|e| e.score <= max);
        end - start
    }

    pub fn pop_min(&mut self) -> Option<ZEntry> {
        if self.entries.is_empty() {
            return None;
        }
        let entry = self.entries.remove(0);
        self.reclaim_capacity();
        Some(entry)
    }

    pub fn pop_max(&mut self) -> Option<ZEntry> {
        let entry = self.entries.pop()?;
        self.reclaim_capacity();
        Some(entry)
    }

    pub fn iter(&self) -> std::slice::Iter<'_, ZEntry> {
        self.entries.iter()
    }

    pub fn iter_rev(&self) -> std::iter::Rev<std::slice::Iter<'_, ZEntry>> {
        self.entries.iter().rev()
    }

    pub fn contains(&self, member: &str) -> bool {
        self.entries.iter().any(|e| e.member.as_str() == member)
    }

    pub fn members(&self) -> impl Iterator<Item = &SmallStr> {
        self.entries.iter().map(|e| &e.member)
    }

    pub fn random_members(&self, count: usize, allow_dup: bool) -> Vec<&ZEntry> {
        use std::collections::HashSet as StdSet;
        if self.entries.is_empty() {
            return vec![];
        }
        let seed = self
            .entries
            .len()
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        let mut rng = seed;
        let mut next = || -> usize {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng % self.entries.len()
        };
        if allow_dup {
            (0..count).map(|_| &self.entries[next()]).collect()
        } else {
            let count = count.min(self.entries.len());
            let mut seen = StdSet::with_capacity(count);
            let mut result = Vec::with_capacity(count);
            while result.len() < count {
                let idx = next();
                if seen.insert(idx) {
                    result.push(&self.entries[idx]);
                }
            }
            result
        }
    }

    pub fn lex_range(
        &self,
        min: &str,
        max: &str,
        min_exclusive: bool,
        max_exclusive: bool,
    ) -> Vec<&ZEntry> {
        self.entries
            .iter()
            .filter(|e| {
                let above_min = if min_exclusive {
                    e.member.as_str() > min
                } else {
                    e.member.as_str() >= min
                };
                let below_max = if max == "+" {
                    true
                } else if max_exclusive {
                    e.member.as_str() < max
                } else {
                    e.member.as_str() <= max
                };
                above_min && below_max
            })
            .collect()
    }

    pub fn count_lex_range(
        &self,
        min: &str,
        max: &str,
        min_exclusive: bool,
        max_exclusive: bool,
    ) -> usize {
        self.lex_range(min, max, min_exclusive, max_exclusive).len()
    }

    pub fn remove_range_by_rank(&mut self, start: usize, end: usize) -> usize {
        let end = end.min(self.entries.len());
        if start >= end {
            return 0;
        }
        self.entries.drain(start..end);
        self.reclaim_capacity();
        end - start
    }

    pub fn remove_range_by_score(&mut self, min: f64, max: f64) -> usize {
        let start = self.entries.partition_point(|e| e.score < min);
        let end = self.entries.partition_point(|e| e.score <= max);
        if start >= end {
            return 0;
        }
        let count = end - start;
        self.entries.drain(start..end);
        self.reclaim_capacity();
        count
    }

    pub fn remove_lex_range(
        &mut self,
        min: &str,
        max: &str,
        min_exclusive: bool,
        max_exclusive: bool,
    ) -> usize {
        let before = self.entries.len();
        self.entries.retain(|e| {
            let above_min = if min_exclusive {
                e.member.as_str() > min
            } else {
                e.member.as_str() >= min
            };
            let below_max = if max == "+" {
                true
            } else if max_exclusive {
                e.member.as_str() < max
            } else {
                e.member.as_str() <= max
            };
            !(above_min && below_max)
        });
        let removed = before - self.entries.len();
        if removed > 0 {
            self.reclaim_capacity();
        }
        removed
    }

    pub fn incr(&mut self, member: &str, increment: f64) -> f64 {
        if let Some(pos) = self
            .entries
            .iter()
            .position(|e| e.member.as_str() == member)
        {
            let new_score = self.entries[pos].score + increment;
            let stays = (pos == 0
                || self.entries[pos - 1].score < new_score
                || (self.entries[pos - 1].score == new_score
                    && self.entries[pos - 1].member.as_str() <= member))
                && (pos >= self.entries.len() - 1
                    || self.entries[pos + 1].score > new_score
                    || (self.entries[pos + 1].score == new_score
                        && self.entries[pos + 1].member.as_str() >= member));
            if stays {
                self.entries[pos].score = new_score;
            } else {
                self.entries.remove(pos);
                let insert_pos = self.find_insert_pos(new_score, member);
                self.entries.insert(
                    insert_pos,
                    ZEntry {
                        score: new_score,
                        member: SmallStr::new(member),
                    },
                );
            }
            new_score
        } else {
            self.insert(increment, member);
            increment
        }
    }

    pub fn shrink_to_fit(&mut self) {
        self.entries.shrink_to_fit();
        self.bloom_rebuild();
    }

    #[inline]
    fn reclaim_capacity(&mut self) {
        if self.entries.capacity() > self.entries.len().saturating_mul(2).max(64) {
            self.entries.shrink_to_fit();
        }
    }

    fn find_insert_pos(&self, score: f64, member: &str) -> usize {
        self.entries.partition_point(|e| {
            e.score < score || (e.score == score && e.member.as_str() < member)
        })
    }
}

impl Default for ZSetData {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod bloom_tests {
    use super::*;

    /// Bloom probes must all land inside the filter, whatever its size.
    #[test]
    fn probes_stay_in_range_for_every_filter_size() {
        for words in [1usize, 2, 8, 64, 1024] {
            let bits = words * 64;
            for seed in 0..512u64 {
                let (h1, h2) = bloom_seeds(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
                for probe in 0..BLOOM_PROBES {
                    assert!(bloom_position(h1, h2, probe, bits) < bits);
                }
            }
        }
    }

    /// A second seed with an even value would revisit the same slots; forcing
    /// it odd is what makes successive probes stride the whole space.
    #[test]
    fn probes_are_distinct_for_a_reasonable_filter() {
        let bits = 4096;
        let mut collisions = 0;
        for seed in 0..1000u64 {
            let (h1, h2) = bloom_seeds(member_hash(&seed.to_string()));
            let mut seen = std::collections::HashSet::new();
            for probe in 0..BLOOM_PROBES {
                if !seen.insert(bloom_position(h1, h2, probe, bits)) {
                    collisions += 1;
                }
            }
        }
        assert!(
            collisions < 50,
            "{collisions} duplicate probe positions across 1000 members"
        );
    }

    /// The filter must never report a member absent when it is present, and its
    /// false-positive rate has to stay low enough that `insert` avoids the
    /// linear membership scan.
    #[test]
    fn no_false_negatives_and_a_low_false_positive_rate() {
        let mut zset = ZSetData::new();
        const PRESENT: usize = 4000;
        for i in 0..PRESENT {
            zset.insert(i as f64, &format!("member-{i}"));
        }
        assert_eq!(zset.len(), PRESENT);

        for i in 0..PRESENT {
            let h = member_hash(&format!("member-{i}"));
            assert!(
                zset.bloom_maybe_contains(h),
                "false negative for member-{i}"
            );
        }

        let mut positives = 0;
        const ABSENT: usize = 20_000;
        for i in 0..ABSENT {
            let h = member_hash(&format!("absent-{i}"));
            if zset.bloom_maybe_contains(h) {
                positives += 1;
            }
        }
        let rate = positives as f64 / ABSENT as f64;
        assert!(
            rate < 0.02,
            "false-positive rate {rate:.4} is high enough to reintroduce the scan"
        );
    }

    /// Re-adding existing members must not duplicate them, which is what the
    /// membership check behind the filter exists to guarantee.
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
}
