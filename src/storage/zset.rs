use crate::storage::store::Store;
use crate::storage::value::{FyroDB, SmallStr, StoreValue, ZSetData};

/// ZADD modifiers, bundled so the flag set has one name at every call site.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ZAddOptions {
    /// Only add new members.
    pub nx: bool,
    /// Only update existing members.
    pub xx: bool,
    /// Only update existing members when the new score is greater.
    pub gt: bool,
    /// Only update existing members when the new score is lower.
    pub lt: bool,
    /// Count changed members (updated + added) in the reply instead of adds only.
    pub ch: bool,
}

impl Store {
/// Plain single-member ZADDs under one lock, per-command added flags;
/// flag variants change replies and stay un-coalesced.
    pub fn zadd_added_flags(
        &self,
        key: &str,
        members: &[(f64, &str)],
    ) -> Result<Vec<bool>, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                let mut z = ZSetData::new();
                let flags: Vec<bool> =
                    members.iter().map(|(s, m)| z.insert(*s, m)).collect();
                val.value = FyroDB::ZSet(Box::new(z));
                val.expires_ms = 0;
                return Ok(flags);
            }
            match val.value.as_zset_mut() {
                Some(z) => Ok(members.iter().map(|(s, m)| z.insert(*s, m)).collect()),
                None => Err("WRONGTYPE"),
            }
        });

        match result {
            Some(r) => r,
            None => {
                let mut z = ZSetData::new();
                let flags: Vec<bool> = members.iter().map(|(s, m)| z.insert(*s, m)).collect();
                self.data.insert_str(
                    key,
                    StoreValue {
                        value: FyroDB::ZSet(Box::new(z)),
                        expires_ms: 0,
                    },
                );
                Ok(flags)
            }
        }
    }

    pub fn zadd(
        &self,
        key: &str,
        members: &[(f64, &str)],
        opts: ZAddOptions,
    ) -> Result<usize, &'static str> {
        let ZAddOptions { nx, xx, gt, lt, ch } = opts;
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                let mut z = ZSetData::new();
                let mut added = 0;
                for (score, member) in members {
                    if !xx {
                        z.insert(*score, member);
                        added += 1;
                    }
                }
                val.value = FyroDB::ZSet(Box::new(z));
                val.expires_ms = 0;
                return Ok(added);
            }
            match val.value.as_zset_mut() {
                Some(z) => {
                    let mut added = 0usize;
                    let mut changed = 0usize;
                    for (score, member) in members {
                        if nx || xx || gt || lt {
                            // Slow path: need to check existing score for conditional logic
                            let existing = z.get_score(member);
                            match existing {
                                Some(old) => {
                                    if nx {
                                        continue;
                                    }
                                    let should_update = if gt && lt {
                                        *score != old
                                    } else if gt {
                                        *score > old
                                    } else if lt {
                                        *score < old
                                    } else {
                                        true
                                    };
                                    if should_update {
                                        z.insert(*score, member);
                                        changed += 1;
                                    }
                                }
                                None => {
                                    if xx {
                                        continue;
                                    }
                                    z.insert(*score, member);
                                    added += 1;
                                }
                            }
                        } else {
                            // Fast path: no flags, just insert directly.
                            // insert() returns true if new, false if updated.
                            if z.insert(*score, member) {
                                added += 1;
                            } else {
                                changed += 1;
                            }
                        }
                    }
                    Ok(if ch { added + changed } else { added })
                }
                None => Err("WRONGTYPE"),
            }
        });

        match result {
            Some(Ok(v)) => Ok(v),
            Some(Err(e)) => Err(e),
            None => {
                if xx {
                    return Ok(0);
                }
                let mut z = ZSetData::new();
                let mut added = 0;
                for (score, member) in members {
                    z.insert(*score, member);
                    added += 1;
                }
                self.data.insert_str(key, StoreValue::zset(z));
                Ok(added)
            }
        }
    }

    pub fn zrem(&self, key: &str, members: &[&str]) -> Result<usize, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Ok(0);
            }
            match val.value.as_zset_mut() {
                Some(z) => Ok(members.iter().filter(|m| z.remove(m).is_some()).count()),
                None => Err("WRONGTYPE"),
            }
        });

        match result {
            Some(Ok(v)) => Ok(v),
            Some(Err(e)) => Err(e),
            None => Ok(0),
        }
    }

    pub fn zscore(&self, key: &str, member: &str) -> Result<Option<f64>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(None),
            Some(e) if e.is_expired() => Ok(None),
            Some(e) => match e.value.as_zset() {
                Some(z) => Ok(z.get_score(member)),
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn zmscore(&self, key: &str, members: &[&str]) -> Result<Vec<Option<f64>>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(vec![None; members.len()]),
            Some(e) if e.is_expired() => Ok(vec![None; members.len()]),
            Some(e) => match e.value.as_zset() {
                Some(z) => Ok(members.iter().map(|m| z.get_score(m)).collect()),
                None => Err("WRONGTYPE"),
            },
        }
    }

/// Nil for missing members; truncation keeps seqlock retries idempotent.
    pub fn zmscore_to_buf(
        &self,
        key: &str,
        members: &[&str],
        out: &mut Vec<u8>,
    ) -> Result<(), &'static str> {
        let start_len = out.len();
        let result = self.data.read_consistent(key, |val| {
            out.truncate(start_len);
            if val.is_expired() {
                crate::utils::resp::write_array_header(out, members.len());
                for _ in members {
                    crate::utils::resp::write_nil(out);
                }
                return Ok(());
            }
            match val.value.as_zset() {
                Some(z) => {
                    crate::utils::resp::write_array_header(out, members.len());
                    for m in members {
                        match z.get_score(m) {
                            Some(score) => crate::utils::resp::write_bulk(
                                out,
                                &crate::utils::util::format_float(score),
                            ),
                            None => crate::utils::resp::write_nil(out),
                        }
                    }
                    Ok(())
                }
                None => Err("WRONGTYPE"),
            }
        });
        match result {
            Some(r) => r,
            None => {
                crate::utils::resp::write_array_header(out, members.len());
                for _ in members {
                    crate::utils::resp::write_nil(out);
                }
                Ok(())
            }
        }
    }

    pub fn zrank(&self, key: &str, member: &str) -> Result<Option<usize>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(None),
            Some(e) if e.is_expired() => Ok(None),
            Some(e) => match e.value.as_zset() {
                Some(z) => Ok(z.rank(member)),
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn zrevrank(&self, key: &str, member: &str) -> Result<Option<usize>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(None),
            Some(e) if e.is_expired() => Ok(None),
            Some(e) => match e.value.as_zset() {
                Some(z) => Ok(z.rev_rank(member)),
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn zcard(&self, key: &str) -> Result<usize, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(0),
            Some(e) if e.is_expired() => Ok(0),
            Some(e) => match e.value.as_zset() {
                Some(z) => Ok(z.len()),
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn zcount(&self, key: &str, min: f64, max: f64) -> Result<usize, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(0),
            Some(e) if e.is_expired() => Ok(0),
            Some(e) => match e.value.as_zset() {
                Some(z) => Ok(z.count_in_score_range(min, max)),
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn zincrby(&self, key: &str, increment: f64, member: &str) -> Result<f64, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                let mut z = ZSetData::new();
                z.insert(increment, member);
                val.value = FyroDB::ZSet(Box::new(z));
                val.expires_ms = 0;
                return Ok(increment);
            }
            match val.value.as_zset_mut() {
                Some(z) => Ok(z.incr(member, increment)),
                None => Err("WRONGTYPE"),
            }
        });

        match result {
            Some(Ok(v)) => Ok(v),
            Some(Err(e)) => Err(e),
            None => {
                let mut z = ZSetData::new();
                z.insert(increment, member);
                self.data.insert_str(key, StoreValue::zset(z));
                Ok(increment)
            }
        }
    }

    pub fn zrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
        _withscores: bool,
    ) -> Result<Vec<(String, f64)>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(vec![]),
            Some(e) if e.is_expired() => Ok(vec![]),
            Some(e) => match e.value.as_zset() {
                Some(z) => {
                    let len = z.len() as i64;
                    let s = normalize_zset_index(start, len);
                    let e_idx = normalize_zset_index(stop, len);
                    if s > e_idx {
                        return Ok(vec![]);
                    }
                    let items: Vec<(String, f64)> = z
                        .range_by_rank(s, e_idx + 1)
                        .iter()
                        .map(|entry| (entry.member.to_string(), entry.score))
                        .collect();
                    Ok(items)
                }
                None => Err("WRONGTYPE"),
            },
        }
    }

    /// Zero-alloc ZRANGE: bulk elements stream straight into `out`. With
    /// scores, each member is followed by its formatted score.
    pub fn zrange_to_buf(
        &self,
        key: &str,
        start: i64,
        stop: i64,
        withscores: bool,
        out: &mut Vec<u8>,
    ) -> Result<usize, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(0),
            Some(e) if e.is_expired() => Ok(0),
            Some(e) => match e.value.as_zset() {
                Some(z) => {
                    let len = z.len() as i64;
                    let s = normalize_zset_index(start, len);
                    let e_idx = normalize_zset_index(stop, len);
                    if s > e_idx {
                        return Ok(0);
                    }
                    let n = e_idx - s + 1;
                    crate::utils::resp::write_array_header(out, if withscores { n * 2 } else { n });
                    for entry in z.range_by_rank(s, e_idx + 1).iter() {
                        crate::utils::resp::write_bulk(out, entry.member.as_str());
                        if withscores {
                            crate::utils::resp::write_bulk(
                                out,
                                &crate::utils::util::format_float(entry.score),
                            );
                        }
                    }
                    Ok(n)
                }
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn zrevrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<(String, f64)>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(vec![]),
            Some(e) if e.is_expired() => Ok(vec![]),
            Some(e) => match e.value.as_zset() {
                Some(z) => {
                    let len = z.len() as i64;
                    let s = normalize_zset_index(start, len);
                    let e_idx = normalize_zset_index(stop, len);
                    if s > e_idx {
                        return Ok(vec![]);
                    }
                    let items: Vec<(String, f64)> = z
                        .iter_rev()
                        .skip(s)
                        .take(e_idx - s + 1)
                        .map(|entry| (entry.member.to_string(), entry.score))
                        .collect();
                    Ok(items)
                }
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn zrangebyscore(
        &self,
        key: &str,
        min: f64,
        max: f64,
        offset: usize,
        count: usize,
    ) -> Result<Vec<(String, f64)>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(vec![]),
            Some(e) if e.is_expired() => Ok(vec![]),
            Some(e) => match e.value.as_zset() {
                Some(z) => {
                    let items: Vec<(String, f64)> = z
                        .range_by_score(min, max)
                        .iter()
                        .skip(offset)
                        .take(if count == 0 { usize::MAX } else { count })
                        .map(|entry| (entry.member.to_string(), entry.score))
                        .collect();
                    Ok(items)
                }
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn zrevrangebyscore(
        &self,
        key: &str,
        max: f64,
        min: f64,
        offset: usize,
        count: usize,
    ) -> Result<Vec<(String, f64)>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(vec![]),
            Some(e) if e.is_expired() => Ok(vec![]),
            Some(e) => match e.value.as_zset() {
                Some(z) => {
                    let items: Vec<(String, f64)> = z
                        .rev_range_by_score(min, max)
                        .into_iter()
                        .skip(offset)
                        .take(if count == 0 { usize::MAX } else { count })
                        .map(|entry| (entry.member.to_string(), entry.score))
                        .collect();
                    Ok(items)
                }
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn zpopmin(&self, key: &str, count: usize) -> Result<Vec<(String, f64)>, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Ok(vec![]);
            }
            match val.value.as_zset_mut() {
                Some(z) => {
                    let n = count.min(z.len());
                    let mut out = Vec::with_capacity(n);
                    for _ in 0..n {
                        if let Some(entry) = z.pop_min() {
                            out.push((entry.member, entry.score));
                        }
                    }
                    Ok(out)
                }
                None => Err("WRONGTYPE"),
            }
        });

        match result {
            Some(Ok(v)) => Ok(v.into_iter().map(|(m, s)| (m.to_string(), s)).collect()),
            Some(Err(e)) => Err(e),
            None => Ok(vec![]),
        }
    }

    pub fn zpopmax(&self, key: &str, count: usize) -> Result<Vec<(String, f64)>, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Ok(vec![]);
            }
            match val.value.as_zset_mut() {
                Some(z) => {
                    let n = count.min(z.len());
                    let mut out = Vec::with_capacity(n);
                    for _ in 0..n {
                        if let Some(entry) = z.pop_max() {
                            out.push((entry.member, entry.score));
                        }
                    }
                    Ok(out)
                }
                None => Err("WRONGTYPE"),
            }
        });

        match result {
            Some(Ok(v)) => Ok(v.into_iter().map(|(m, s)| (m.to_string(), s)).collect()),
            Some(Err(e)) => Err(e),
            None => Ok(vec![]),
        }
    }

    pub fn zunionstore(
        &self,
        dst: &str,
        keys: &[&str],
        weights: &[f64],
        aggregate: ZAggregate,
    ) -> Result<usize, &'static str> {
        let mut result = ZSetData::new();
        for (i, &k) in keys.iter().enumerate() {
            let weight = weights.get(i).copied().unwrap_or(1.0);
            match self.data.get_ref(k) {
                None => {}
                Some(e) if e.is_expired() => {}
                Some(e) => match e.value.as_zset() {
                    Some(z) => {
                        for entry in z.iter() {
                            let weighted = entry.score * weight;
                            let new_score = match result.get_score(&entry.member) {
                                Some(existing) => aggregate.apply(existing, weighted),
                                None => weighted,
                            };
                            result.insert(new_score, entry.member.as_str());
                        }
                    }
                    None => return Err("WRONGTYPE"),
                },
            }
        }
        let count = result.len();
        self.data.insert(dst.to_string(), StoreValue::zset(result));
        Ok(count)
    }

    pub fn zinterstore(
        &self,
        dst: &str,
        keys: &[&str],
        weights: &[f64],
        aggregate: ZAggregate,
    ) -> Result<usize, &'static str> {
        if keys.is_empty() {
            self.data
                .insert(dst.to_string(), StoreValue::zset(ZSetData::new()));
            return Ok(0);
        }
        // Collect first set's members without cloning the full ZSetData.
        let first_members: Vec<(SmallStr, f64)> = match self.data.get_ref(keys[0]) {
            None => {
                self.data
                    .insert(dst.to_string(), StoreValue::zset(ZSetData::new()));
                return Ok(0);
            }
            Some(e) if e.is_expired() => {
                self.data
                    .insert(dst.to_string(), StoreValue::zset(ZSetData::new()));
                return Ok(0);
            }
            Some(e) => match e.value.as_zset() {
                Some(z) => z
                    .iter()
                    .map(|entry| (entry.member.clone(), entry.score))
                    .collect(),
                None => return Err("WRONGTYPE"),
            },
        };

        let w0 = weights.first().copied().unwrap_or(1.0);
        let mut result = ZSetData::new();
        for (member, score) in &first_members {
            result.insert(*score * w0, member.as_str());
        }

        for (i, &k) in keys[1..].iter().enumerate() {
            let weight = weights.get(i + 1).copied().unwrap_or(1.0);
            // Collect other set's scores without cloning full ZSetData.
            let other_scores: Vec<(SmallStr, f64)> = match self.data.get_ref(k) {
                None => {
                    self.data
                        .insert(dst.to_string(), StoreValue::zset(ZSetData::new()));
                    return Ok(0);
                }
                Some(e) if e.is_expired() => {
                    self.data
                        .insert(dst.to_string(), StoreValue::zset(ZSetData::new()));
                    return Ok(0);
                }
                Some(e) => match e.value.as_zset() {
                    Some(z) => z
                        .iter()
                        .map(|entry| (entry.member.clone(), entry.score))
                        .collect(),
                    None => return Err("WRONGTYPE"),
                },
            };

            let mut next = ZSetData::new();
            for entry in result.iter() {
                if let Some(other_score) = other_scores
                    .iter()
                    .find(|(m, _)| m.as_str() == entry.member.as_str())
                    .map(|(_, s)| *s)
                {
                    let new_score = aggregate.apply(entry.score, other_score * weight);
                    next.insert(new_score, entry.member.as_str());
                }
            }
            result = next;
        }

        let count = result.len();
        self.data.insert(dst.to_string(), StoreValue::zset(result));
        Ok(count)
    }

    pub fn zrandmember(&self, key: &str, count: i64) -> Result<Vec<(String, f64)>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(vec![]),
            Some(e) if e.is_expired() => Ok(vec![]),
            Some(e) => match e.value.as_zset() {
                Some(z) => {
                    if z.is_empty() {
                        return Ok(vec![]);
                    }
                    if count >= 0 {
                        let entries = z.random_members(count as usize, false);
                        Ok(entries
                            .iter()
                            .map(|e| (e.member.to_string(), e.score))
                            .collect())
                    } else {
                        let entries = z.random_members((-count) as usize, true);
                        Ok(entries
                            .iter()
                            .map(|e| (e.member.to_string(), e.score))
                            .collect())
                    }
                }
                None => Err("WRONGTYPE"),
            },
        }
    }
}

#[derive(Clone, Copy)]
pub enum ZAggregate {
    Sum,
    Min,
    Max,
}

impl ZAggregate {
    #[inline]
    pub fn apply(self, a: f64, b: f64) -> f64 {
        match self {
            Self::Sum => a + b,
            Self::Min => a.min(b),
            Self::Max => a.max(b),
        }
    }

    pub fn parse_str(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("SUM") {
            Some(Self::Sum)
        } else if s.eq_ignore_ascii_case("MIN") {
            Some(Self::Min)
        } else if s.eq_ignore_ascii_case("MAX") {
            Some(Self::Max)
        } else {
            None
        }
    }
}

impl Store {
    pub fn zlexcount(&self, key: &str, min: &str, max: &str) -> Result<usize, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(0),
            Some(e) if e.is_expired() => Ok(0),
            Some(e) => match e.value.as_zset() {
                Some(z) => {
                    let (min_val, min_exc) = parse_lex_bound_min(min);
                    let (max_val, max_exc) = parse_lex_bound_max(max);
                    Ok(z.count_lex_range(min_val, max_val, min_exc, max_exc))
                }
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn zrangebylex(
        &self,
        key: &str,
        min: &str,
        max: &str,
        offset: usize,
        count: usize,
    ) -> Result<Vec<String>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(vec![]),
            Some(e) if e.is_expired() => Ok(vec![]),
            Some(e) => match e.value.as_zset() {
                Some(z) => {
                    let (min_val, min_exc) = parse_lex_bound_min(min);
                    let (max_val, max_exc) = parse_lex_bound_max(max);
                    let members: Vec<String> = z
                        .lex_range(min_val, max_val, min_exc, max_exc)
                        .into_iter()
                        .skip(offset)
                        .take(if count == 0 { usize::MAX } else { count })
                        .map(|entry| entry.member.to_string())
                        .collect();
                    Ok(members)
                }
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn zdiff(&self, keys: &[&str]) -> Result<Vec<(String, f64)>, &'static str> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let first = match self.data.get_ref(keys[0]) {
            None => return Ok(vec![]),
            Some(e) if e.is_expired() => return Ok(vec![]),
            Some(e) => match e.value.as_zset() {
                Some(z) => z.clone(),
                None => return Err("WRONGTYPE"),
            },
        };

        let mut result = first;
        for &k in &keys[1..] {
            match self.data.get_ref(k) {
                None => {}
                Some(e) if e.is_expired() => {}
                Some(e) => match e.value.as_zset() {
                    Some(z) => {
                        let to_remove: Vec<String> = result
                            .members()
                            .filter(|m| z.contains(m))
                            .map(|m| m.to_string())
                            .collect();
                        for m in to_remove {
                            result.remove(&m);
                        }
                    }
                    None => return Err("WRONGTYPE"),
                },
            }
        }

        let items: Vec<(String, f64)> = result
            .iter()
            .map(|entry| (entry.member.to_string(), entry.score))
            .collect();
        Ok(items)
    }

    pub fn zdiffstore(&self, dst: &str, keys: &[&str]) -> Result<usize, &'static str> {
        let items = self.zdiff(keys)?;
        let mut z = ZSetData::new();
        for (member, score) in &items {
            z.insert(*score, member.as_str());
        }
        let count = z.len();
        self.data.insert(dst.to_string(), StoreValue::zset(z));
        Ok(count)
    }

    pub fn zunion(
        &self,
        keys: &[&str],
        weights: &[f64],
        aggregate: ZAggregate,
    ) -> Result<Vec<(String, f64)>, &'static str> {
        let mut result = ZSetData::new();
        for (i, &k) in keys.iter().enumerate() {
            let weight = weights.get(i).copied().unwrap_or(1.0);
            match self.data.get_ref(k) {
                None => {}
                Some(e) if e.is_expired() => {}
                Some(e) => match e.value.as_zset() {
                    Some(z) => {
                        for entry in z.iter() {
                            let weighted = entry.score * weight;
                            let new_score = match result.get_score(&entry.member) {
                                Some(existing) => aggregate.apply(existing, weighted),
                                None => weighted,
                            };
                            result.insert(new_score, entry.member.as_str());
                        }
                    }
                    None => return Err("WRONGTYPE"),
                },
            }
        }
        let items: Vec<(String, f64)> = result
            .iter()
            .map(|entry| (entry.member.to_string(), entry.score))
            .collect();
        Ok(items)
    }

    pub fn zinter(
        &self,
        keys: &[&str],
        weights: &[f64],
        aggregate: ZAggregate,
    ) -> Result<Vec<(String, f64)>, &'static str> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let first = match self.data.get_ref(keys[0]) {
            None => return Ok(vec![]),
            Some(e) if e.is_expired() => return Ok(vec![]),
            Some(e) => match e.value.as_zset() {
                Some(z) => z.clone(),
                None => return Err("WRONGTYPE"),
            },
        };

        let w0 = weights.first().copied().unwrap_or(1.0);
        let mut result = ZSetData::new();
        for entry in first.iter() {
            result.insert(entry.score * w0, entry.member.as_str());
        }

        for (i, &k) in keys[1..].iter().enumerate() {
            let weight = weights.get(i + 1).copied().unwrap_or(1.0);
            let other = match self.data.get_ref(k) {
                None => return Ok(vec![]),
                Some(e) if e.is_expired() => return Ok(vec![]),
                Some(e) => match e.value.as_zset() {
                    Some(z) => z.clone(),
                    None => return Err("WRONGTYPE"),
                },
            };

            let mut next = ZSetData::new();
            for entry in result.iter() {
                if let Some(other_score) = other.get_score(&entry.member) {
                    let new_score = aggregate.apply(entry.score, other_score * weight);
                    next.insert(new_score, entry.member.as_str());
                }
            }
            result = next;
        }

        let items: Vec<(String, f64)> = result
            .iter()
            .map(|entry| (entry.member.to_string(), entry.score))
            .collect();
        Ok(items)
    }
}

fn parse_lex_bound_min(s: &str) -> (&str, bool) {
    if s == "-" {
        ("", false)
    } else if let Some(m) = s.strip_prefix('(') {
        (m, true)
    } else if let Some(m) = s.strip_prefix('[') {
        (m, false)
    } else {
        (s, false)
    }
}

fn parse_lex_bound_max(s: &str) -> (&str, bool) {
    if s == "+" {
        ("+", false)
    } else if let Some(m) = s.strip_prefix('(') {
        (m, true)
    } else if let Some(m) = s.strip_prefix('[') {
        (m, false)
    } else {
        (s, false)
    }
}

#[inline]
fn normalize_zset_index(index: i64, len: i64) -> usize {
    if index < 0 {
        (len + index).max(0) as usize
    } else {
        index.min(len.saturating_sub(1)) as usize
    }
}
