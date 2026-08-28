use crate::storage::store::Store;
use crate::storage::value::{FyroDB, ListInner, StoreValue};
use std::collections::VecDeque;

impl Store {
    pub fn lpush(&self, key: &str, values: &[&str]) -> Result<usize, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                let mut l = VecDeque::with_capacity(values.len());
                for v in values.iter().rev() {
                    l.push_front(crate::storage::value::SmallStr::new(v));
                }
                let len = l.len();
                val.value = FyroDB::List(Box::new(ListInner::Compact(l)));
                val.expires_ms = 0;
                return Ok(len);
            }
            match val.value.as_list_mut() {
                Some(l) => {
                    for v in values {
                        l.push_front(crate::storage::value::SmallStr::new(v));
                    }
                    Ok(l.len())
                }
                None => Err("WRONGTYPE"),
            }
        });

        match result {
            Some(r) => r,
            None => {
                let mut l = VecDeque::with_capacity(values.len());
                for v in values {
                    l.push_front(crate::storage::value::SmallStr::new(v));
                }
                let len = l.len();
                self.data.insert(
                    key.to_string(),
                    StoreValue {
                        value: FyroDB::List(Box::new(ListInner::Compact(l))),
                        expires_ms: 0,
                    },
                );
                Ok(len)
            }
        }
    }

    pub fn rpush(&self, key: &str, values: &[&str]) -> Result<usize, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                let mut l = VecDeque::with_capacity(values.len());
                for v in values {
                    l.push_back(crate::storage::value::SmallStr::new(v));
                }
                let len = l.len();
                val.value = FyroDB::List(Box::new(ListInner::Compact(l)));
                val.expires_ms = 0;
                return Ok(len);
            }
            match val.value.as_list_mut() {
                Some(l) => {
                    for v in values {
                        l.push_back(crate::storage::value::SmallStr::new(v));
                    }
                    Ok(l.len())
                }
                None => Err("WRONGTYPE"),
            }
        });

        match result {
            Some(r) => r,
            None => {
                let mut l = VecDeque::with_capacity(values.len());
                for v in values {
                    l.push_back(v.to_string());
                }
                let len = l.len();
                self.data.insert(key.to_string(), StoreValue::list(l));
                Ok(len)
            }
        }
    }

    pub fn lpop(&self, key: &str, count: usize) -> Result<Vec<String>, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Ok(vec![]);
            }
            match val.value.as_list_mut() {
                Some(l) => {
                    let n = count.min(l.len());
                    let mut out = Vec::with_capacity(n);
                    for _ in 0..n {
                        if let Some(v) = l.pop_front() {
                            out.push(v.into_string());
                        }
                    }
                    if l.is_empty() && l.capacity() > Self::LIST_KEEP_CAPACITY {
                        l.shrink_to_fit();
                    }
                    Ok(out)
                }
                None => Err("WRONGTYPE"),
            }
        });

        match result {
            Some(r) => r,
            None => Ok(vec![]),
        }
    }

    pub fn rpop(&self, key: &str, count: usize) -> Result<Vec<String>, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Ok(vec![]);
            }
            match val.value.as_list_mut() {
                Some(l) => {
                    let n = count.min(l.len());
                    let mut out = Vec::with_capacity(n);
                    for _ in 0..n {
                        if let Some(v) = l.pop_back() {
                            out.push(v.into_string());
                        }
                    }
                    if l.is_empty() && l.capacity() > Self::LIST_KEEP_CAPACITY {
                        l.shrink_to_fit();
                    }
                    Ok(out)
                }
                None => Err("WRONGTYPE"),
            }
        });

        match result {
            Some(r) => r,
            None => Ok(vec![]),
        }
    }

    /// Capacity below which an emptied list keeps its buffer.
    ///
    /// A queue workload drains to empty constantly, and calling
    /// `shrink_to_fit` every time turns each drain into a free plus a
    /// reallocation inside the entry lock. Only oversized buffers are worth
    /// releasing.
    const LIST_KEEP_CAPACITY: usize = 64;

    /// Pop one element and write it straight into the reply buffer.
    ///
    /// The `Vec<String>`-returning form allocates a vector *and* a string per
    /// popped element, both inside the entry lock, then the caller copies the
    /// bytes out and drops them. For the single-element pops that dominate queue
    /// traffic none of that is needed.
    ///
    /// `Ok(true)` means an element was written, `Ok(false)` that the list was
    /// missing, expired or empty.
    pub fn pop_one_to_buf(
        &self,
        key: &str,
        from_back: bool,
        out: &mut Vec<u8>,
    ) -> Result<bool, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Ok(false);
            }
            let Some(list) = val.value.as_list_mut() else {
                return Err("WRONGTYPE");
            };
            let popped = if from_back {
                list.pop_back()
            } else {
                list.pop_front()
            };
            let Some(value) = popped else {
                return Ok(false);
            };
            crate::utils::resp::write_bulk(out, value.as_str());
            if list.is_empty() && list.capacity() > Self::LIST_KEEP_CAPACITY {
                list.shrink_to_fit();
            }
            Ok(true)
        });
        result.unwrap_or(Ok(false))
    }

    pub fn llen(&self, key: &str) -> Result<usize, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(0),
            Some(e) if e.is_expired() => Ok(0),
            Some(e) => match e.value.as_list() {
                Some(l) => Ok(l.len()),
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn lindex(&self, key: &str, index: i64) -> Result<Option<String>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(None),
            Some(e) if e.is_expired() => Ok(None),
            Some(e) => match e.value.as_list() {
                Some(l) => {
                    let idx = normalize_index(index, l.len());
                    Ok(idx.and_then(|i| l.get(i).map(|v| v.to_string())))
                }
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn lset(&self, key: &str, index: i64, value: &str) -> Result<bool, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Err("no such key");
            }
            match val.value.as_list_mut() {
                Some(l) => {
                    let idx = normalize_index(index, l.len());
                    match idx {
                        Some(i) => {
                            l[i] = crate::storage::value::SmallStr::new(value);
                            Ok(true)
                        }
                        None => Err("index out of range"),
                    }
                }
                None => Err("WRONGTYPE"),
            }
        });

        match result {
            Some(r) => r,
            None => Err("no such key"),
        }
    }

    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, &'static str> {
        let result = self.data.read_consistent(key, |val| {
            if val.is_expired() {
                return Ok(vec![]);
            }
            match val.value.as_list() {
                Some(l) => {
                    let len = l.len() as i64;
                    let s = if start < 0 {
                        (len + start).max(0)
                    } else {
                        start.min(len)
                    } as usize;
                    let e_idx = if stop < 0 {
                        (len + stop).max(0)
                    } else {
                        stop.min(len - 1)
                    } as usize;
                    if s > e_idx {
                        return Ok(vec![]);
                    }
                    Ok(l.iter()
                        .skip(s)
                        .take(e_idx - s + 1)
                        .map(|v| v.to_string())
                        .collect())
                }
                None => Err("WRONGTYPE"),
            }
        });
        match result {
            Some(r) => r,
            None => Ok(vec![]),
        }
    }

    pub fn ltrim(&self, key: &str, start: i64, stop: i64) -> Result<(), &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Ok(());
            }
            match val.value.as_list_mut() {
                Some(l) => {
                    let len = l.len() as i64;
                    let s = if start < 0 {
                        (len + start).max(0)
                    } else {
                        start.min(len)
                    } as usize;
                    let e = if stop < 0 {
                        (len + stop).max(0)
                    } else {
                        stop.min(len - 1)
                    } as usize;
                    if s > e || s >= l.len() {
                        l.clear();
                        l.shrink_to_fit();
                    } else {
                        l.drain(..s);
                        let keep = (e - s + 1).min(l.len());
                        l.truncate(keep);
                        // VecDeque keeps removed storage by default; release it after
                        // destructive trims so queue workloads do not retain peak RSS.
                        if l.capacity() > l.len().saturating_mul(2).max(64) {
                            l.shrink_to_fit();
                        }
                    }
                    Ok(())
                }
                None => Err("WRONGTYPE"),
            }
        });

        match result {
            Some(r) => r,
            None => Ok(()),
        }
    }

    pub fn lrem(&self, key: &str, count: i64, value: &str) -> Result<usize, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Ok(0);
            }
            match val.value.as_list_mut() {
                Some(l) => {
                    let mut removed = 0usize;
                    if count > 0 {
                        let limit = count as usize;
                        let mut i = 0;
                        while i < l.len() && removed < limit {
                            if l[i] == value {
                                l.remove(i);
                                removed += 1;
                            } else {
                                i += 1;
                            }
                        }
                    } else if count < 0 {
                        let limit = (-count) as usize;
                        let mut i = l.len();
                        while i > 0 && removed < limit {
                            i -= 1;
                            if l[i] == value {
                                l.remove(i);
                                removed += 1;
                            }
                        }
                    } else {
                        l.retain(|v| {
                            if v == value {
                                removed += 1;
                                false
                            } else {
                                true
                            }
                        });
                    }
                    if l.is_empty() || l.capacity() > l.len().saturating_mul(2).max(64) {
                        l.shrink_to_fit();
                    }
                    Ok(removed)
                }
                None => Err("WRONGTYPE"),
            }
        });

        match result {
            Some(r) => r,
            None => Ok(0),
        }
    }

    pub fn linsert(
        &self,
        key: &str,
        before: bool,
        pivot: &str,
        value: &str,
    ) -> Result<i64, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Ok(-1i64);
            }
            match val.value.as_list_mut() {
                Some(l) => {
                    let pos = l.iter().position(|v| v == pivot);
                    match pos {
                        Some(idx) => {
                            let insert_at = if before { idx } else { idx + 1 };
                            l.insert(insert_at, crate::storage::value::SmallStr::new(value));
                            Ok(l.len() as i64)
                        }
                        None => Ok(-1),
                    }
                }
                None => Err("WRONGTYPE"),
            }
        });

        match result {
            Some(r) => r,
            None => Ok(0),
        }
    }

    pub fn lpos(
        &self,
        key: &str,
        value: &str,
        rank: i64,
        count: usize,
        maxlen: usize,
    ) -> Result<Vec<usize>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(vec![]),
            Some(e) if e.is_expired() => Ok(vec![]),
            Some(e) => match e.value.as_list() {
                Some(l) => {
                    let mut results = Vec::new();
                    let max = if maxlen == 0 {
                        l.len()
                    } else {
                        maxlen.min(l.len())
                    };

                    if rank >= 0 {
                        let skip = if rank == 0 { 0 } else { (rank - 1) as usize };
                        let mut found = 0usize;
                        for (i, v) in l.iter().enumerate().take(max) {
                            if v == value {
                                if found >= skip {
                                    results.push(i);
                                    if count > 0 && results.len() >= count {
                                        break;
                                    }
                                }
                                found += 1;
                            }
                        }
                    } else {
                        let skip = ((-rank) - 1) as usize;
                        let mut found = 0usize;
                        let start = if max < l.len() { l.len() - max } else { 0 };
                        for i in (start..l.len()).rev() {
                            if l[i] == value {
                                if found >= skip {
                                    results.push(i);
                                    if count > 0 && results.len() >= count {
                                        break;
                                    }
                                }
                                found += 1;
                            }
                        }
                    }
                    Ok(results)
                }
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn lmove(
        &self,
        src: &str,
        dst: &str,
        left_src: bool,
        left_dst: bool,
    ) -> Result<Option<String>, &'static str> {
        let popped = self.data.update_with(src, |val| {
            if val.is_expired() {
                return Ok(None);
            }
            match val.value.as_list_mut() {
                Some(l) => {
                    let v = if left_src {
                        l.pop_front()
                    } else {
                        l.pop_back()
                    };
                    Ok(v)
                }
                None => Err("WRONGTYPE"),
            }
        });

        let value = match popped {
            Some(Ok(Some(v))) => v,
            Some(Ok(None)) => return Ok(None),
            Some(Err(e)) => return Err(e),
            None => return Ok(None),
        };

        if src == dst {
            let result = self
                .data
                .update_with(dst, |val| match val.value.as_list_mut() {
                    Some(l) => {
                        if left_dst {
                            l.push_front(value.clone());
                        } else {
                            l.push_back(value.clone());
                        }
                        Ok(())
                    }
                    None => Err("WRONGTYPE"),
                });
            match result {
                Some(Ok(())) => {}
                Some(Err(e)) => return Err(e),
                None => {
                    let mut l = VecDeque::new();
                    l.push_back(value.clone());
                    self.data.insert(
                        dst.to_string(),
                        StoreValue {
                            value: FyroDB::List(Box::new(ListInner::Compact(l))),
                            expires_ms: 0,
                        },
                    );
                }
            }
        } else {
            let push_result = self.data.update_with(dst, |val| {
                if val.is_expired() {
                    let mut l = VecDeque::new();
                    if left_dst {
                        l.push_front(value.clone());
                    } else {
                        l.push_back(value.clone());
                    }
                    val.value = FyroDB::List(Box::new(ListInner::Compact(l)));
                    val.expires_ms = 0;
                    return Ok(());
                }
                match val.value.as_list_mut() {
                    Some(l) => {
                        if left_dst {
                            l.push_front(value.clone());
                        } else {
                            l.push_back(value.clone());
                        }
                        Ok(())
                    }
                    None => Err("WRONGTYPE"),
                }
            });

            match push_result {
                Some(Ok(())) => {}
                Some(Err(e)) => return Err(e),
                None => {
                    let mut l = VecDeque::new();
                    if left_dst {
                        l.push_front(value.clone());
                    } else {
                        l.push_back(value.clone());
                    }
                    self.data.insert(
                        dst.to_string(),
                        StoreValue {
                            value: FyroDB::List(Box::new(ListInner::Compact(l))),
                            expires_ms: 0,
                        },
                    );
                }
            }
        }

        Ok(Some(value.into_string()))
    }
}

#[inline]
fn normalize_index(index: i64, len: usize) -> Option<usize> {
    let i = if index < 0 {
        let adjusted = len as i64 + index;
        if adjusted < 0 {
            return None;
        }
        adjusted as usize
    } else {
        index as usize
    };
    if i < len { Some(i) } else { None }
}
