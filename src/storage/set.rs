use crate::storage::store::Store;
use crate::storage::value::{FyroDB, SetInner, StoreValue};
use foldhash::{HashSet, HashSetExt};

impl Store {
    pub fn sadd(&self, key: &str, members: &[&str]) -> Result<usize, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                let mut set = SetInner::new();
                let added = members.iter().filter(|m| set.insert_str(m)).count();
                val.value = FyroDB::Set(Box::new(set));
                val.expires_ms = 0;
                return Ok(added);
            }
            match val.value.as_set_mut() {
                Some(s) => {
                    let mut added = 0;
                    for m in members {
                        if s.insert_str(m) {
                            added += 1;
                        }
                    }
                    Ok(added)
                }
                None => Err("WRONGTYPE"),
            }
        });

        match result {
            Some(r) => r,
            None => {
                let mut set = SetInner::new();
                let added = members.iter().filter(|m| set.insert_str(m)).count();
                self.data.insert(
                    key.to_string(),
                    StoreValue {
                        value: FyroDB::Set(Box::new(set)),
                        expires_ms: 0,
                    },
                );
                Ok(added)
            }
        }
    }

    pub fn srem(&self, key: &str, members: &[&str]) -> Result<usize, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Ok(0);
            }
            match val.value.as_set_mut() {
                Some(s) => {
                    let removed = members.iter().filter(|m| s.remove(m)).count();
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

    pub fn sismember(&self, key: &str, member: &str) -> Result<bool, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(false),
            Some(e) if e.is_expired() => Ok(false),
            Some(e) => match e.value.as_set() {
                Some(s) => Ok(s.contains(member)),
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn smismember(&self, key: &str, members: &[&str]) -> Result<Vec<bool>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(vec![false; members.len()]),
            Some(e) if e.is_expired() => Ok(vec![false; members.len()]),
            Some(e) => match e.value.as_set() {
                Some(s) => Ok(members.iter().map(|m| s.contains(m)).collect()),
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn smembers(&self, key: &str) -> Result<Vec<String>, &'static str> {
        let result = self.data.read_consistent(key, |val| {
            if val.is_expired() {
                return Ok(vec![]);
            }
            match val.value.as_set() {
                Some(s) => Ok(s.iter().map(|m| m.to_string()).collect()),
                None => Err("WRONGTYPE"),
            }
        });
        match result {
            Some(r) => r,
            None => Ok(vec![]),
        }
    }

    pub fn scard(&self, key: &str) -> Result<usize, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(0),
            Some(e) if e.is_expired() => Ok(0),
            Some(e) => match e.value.as_set() {
                Some(s) => Ok(s.len()),
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn spop(&self, key: &str, count: usize) -> Result<Vec<String>, &'static str> {
        let result = self.data.update_with(key, |val| {
            if val.is_expired() {
                return Ok(vec![]);
            }
            match val.value.as_set_mut() {
                Some(s) => {
                    let n = count.min(s.len());
                    let mut out = Vec::with_capacity(n);
                    for _ in 0..n {
                        let member = s.iter().next().map(|m| m.to_string());
                        if let Some(m) = member {
                            s.remove(&m);
                            out.push(m);
                        }
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

    pub fn srandmember(&self, key: &str, count: i64) -> Result<Vec<String>, &'static str> {
        match self.data.get_ref(key) {
            None => Ok(vec![]),
            Some(e) if e.is_expired() => Ok(vec![]),
            Some(e) => match e.value.as_set() {
                Some(s) => {
                    if s.is_empty() {
                        return Ok(vec![]);
                    }
                    let members: Vec<String> = s.iter().map(|m| m.to_string()).collect();
                    if count >= 0 {
                        let n = (count as usize).min(members.len());
                        Ok(members.iter().take(n).map(|m| m.to_string()).collect())
                    } else {
                        let n = (-count) as usize;
                        let mut out = Vec::with_capacity(n);
                        let seed = crate::storage::value::now_ms();
                        for i in 0..n {
                            let idx = ((seed.wrapping_add(i as u64))
                                .wrapping_mul(0x9e3779b97f4a7c15))
                                as usize
                                % members.len();
                            out.push(members[idx].to_string());
                        }
                        Ok(out)
                    }
                }
                None => Err("WRONGTYPE"),
            },
        }
    }

    pub fn smove(&self, src: &str, dst: &str, member: &str) -> Result<bool, &'static str> {
        let removed = self.data.update_with(src, |val| {
            if val.is_expired() {
                return Ok(false);
            }
            match val.value.as_set_mut() {
                Some(s) => Ok(s.remove(member)),
                None => Err("WRONGTYPE"),
            }
        });

        match removed {
            Some(Ok(true)) => {}
            Some(Ok(false)) => return Ok(false),
            Some(Err(e)) => return Err(e),
            None => return Ok(false),
        }

        let add_result = self.data.update_with(dst, |val| {
            if val.is_expired() {
                let mut set = SetInner::new();
                set.insert(member.to_string());
                val.value = FyroDB::Set(Box::new(set));
                val.expires_ms = 0;
                return Ok(());
            }
            match val.value.as_set_mut() {
                Some(s) => {
                    s.insert(member.to_string());
                    Ok(())
                }
                None => Err("WRONGTYPE"),
            }
        });

        match add_result {
            Some(Ok(())) => Ok(true),
            Some(Err(e)) => Err(e),
            None => {
                let mut s = HashSet::new();
                s.insert(member.to_string());
                self.data.insert(dst.to_string(), StoreValue::set(s));
                Ok(true)
            }
        }
    }

    pub fn sunion(&self, keys: &[&str]) -> Result<Vec<String>, &'static str> {
        let mut result = HashSet::new();
        for &k in keys {
            match self.data.get_ref(k) {
                None => {}
                Some(e) if e.is_expired() => {}
                Some(e) => match e.value.as_set() {
                    Some(s) => {
                        for m in s.iter() {
                            result.insert(m.to_string());
                        }
                    }
                    None => return Err("WRONGTYPE"),
                },
            }
        }
        Ok(result.into_iter().map(|m| m.to_string()).collect())
    }

    pub fn sinter(&self, keys: &[&str]) -> Result<Vec<String>, &'static str> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let first: HashSet<String> = match self.data.get_ref(keys[0]) {
            None => return Ok(vec![]),
            Some(e) if e.is_expired() => return Ok(vec![]),
            Some(e) => match e.value.as_set() {
                Some(s) => s.iter().map(|m| m.to_string()).collect(),
                None => return Err("WRONGTYPE"),
            },
        };

        let mut result: HashSet<String> = first;
        for &k in &keys[1..] {
            match self.data.get_ref(k) {
                None => return Ok(vec![]),
                Some(e) if e.is_expired() => return Ok(vec![]),
                Some(e) => match e.value.as_set() {
                    Some(s) => {
                        result.retain(|m| s.contains(m));
                    }
                    None => return Err("WRONGTYPE"),
                },
            }
            if result.is_empty() {
                break;
            }
        }
        Ok(result.into_iter().map(|m| m.to_string()).collect())
    }

    pub fn sdiff(&self, keys: &[&str]) -> Result<Vec<String>, &'static str> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let first: HashSet<String> = match self.data.get_ref(keys[0]) {
            None => return Ok(vec![]),
            Some(e) if e.is_expired() => return Ok(vec![]),
            Some(e) => match e.value.as_set() {
                Some(s) => s.iter().map(|m| m.to_string()).collect(),
                None => return Err("WRONGTYPE"),
            },
        };

        let mut result: HashSet<String> = first;
        for &k in &keys[1..] {
            match self.data.get_ref(k) {
                None => {}
                Some(e) if e.is_expired() => {}
                Some(e) => match e.value.as_set() {
                    Some(s) => {
                        result.retain(|m| !s.contains(m));
                    }
                    None => return Err("WRONGTYPE"),
                },
            }
        }
        Ok(result.into_iter().map(|m| m.to_string()).collect())
    }

    pub fn sunionstore(&self, dst: &str, keys: &[&str]) -> Result<usize, &'static str> {
        let members = self.sunion(keys)?;
        let count = members.len();
        let s: HashSet<String> = members.into_iter().collect();
        self.data.insert(dst.to_string(), StoreValue::set(s));
        Ok(count)
    }

    pub fn sinterstore(&self, dst: &str, keys: &[&str]) -> Result<usize, &'static str> {
        let members = self.sinter(keys)?;
        let count = members.len();
        let s: HashSet<String> = members.into_iter().collect();
        self.data.insert(dst.to_string(), StoreValue::set(s));
        Ok(count)
    }

    pub fn sdiffstore(&self, dst: &str, keys: &[&str]) -> Result<usize, &'static str> {
        let members = self.sdiff(keys)?;
        let count = members.len();
        let s: HashSet<String> = members.into_iter().collect();
        self.data.insert(dst.to_string(), StoreValue::set(s));
        Ok(count)
    }

    pub fn sintercard(&self, keys: &[&str], limit: usize) -> Result<usize, &'static str> {
        let members = self.sinter(keys)?;
        if limit > 0 {
            Ok(members.len().min(limit))
        } else {
            Ok(members.len())
        }
    }
}
