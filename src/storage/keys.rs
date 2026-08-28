use crate::storage::store::Store;
use crate::storage::value::now_ms;
use crate::utils::util::glob_match;
use std::time::Duration;

impl Store {
    pub fn del(&self, key: &str) -> bool {
        let removed = self.data.remove_no_clone(key);
        if removed && let Some(log) = &self.replication {
            let _ = log.append(crate::cluster::MutationRecord {
                offset: 0,
                slot: crate::cluster::hash_slot(key.as_bytes()),
                kind: crate::cluster::MutationKind::Delete,
                key: key.as_bytes().to_vec(),
                value: Vec::new(),
                expire_at_ms: None,
            });
        }
        removed
    }

    pub fn exists(&self, key: &str) -> bool {
        matches!(self.data.get_ref(key), Some(e) if !e.is_expired_precise())
    }

    pub fn expire(&self, key: &str, duration: Duration) -> bool {
        let ok = self
            .data
            .update_with(key, |val| {
                if val.is_expired() {
                    return false;
                }
                let was_persistent = val.expires_ms == 0;
                val.expires_ms = now_ms() + duration.as_millis() as u64;
                was_persistent
            })
            .unwrap_or(false);
        if ok {
            self.add_ttl();
            if let Some(log) = &self.replication {
                let _ = log.append(crate::cluster::MutationRecord {
                    offset: 0,
                    slot: crate::cluster::hash_slot(key.as_bytes()),
                    kind: crate::cluster::MutationKind::Expire,
                    key: key.as_bytes().to_vec(),
                    value: Vec::new(),
                    expire_at_ms: self.data.get_ref(key).map(|value| value.expires_ms),
                });
            }
        }
        ok
    }

    pub fn expire_ms(&self, key: &str, abs_ms: u64) -> bool {
        let ok = self
            .data
            .update_with(key, |val| {
                if val.is_expired() {
                    return false;
                }
                let was_persistent = val.expires_ms == 0;
                val.expires_ms = abs_ms;
                was_persistent
            })
            .unwrap_or(false);
        if ok {
            self.add_ttl();
            if let Some(log) = &self.replication {
                let _ = log.append(crate::cluster::MutationRecord {
                    offset: 0,
                    slot: crate::cluster::hash_slot(key.as_bytes()),
                    kind: crate::cluster::MutationKind::Expire,
                    key: key.as_bytes().to_vec(),
                    value: Vec::new(),
                    expire_at_ms: Some(abs_ms),
                });
            }
        }
        ok
    }

    pub fn persist(&self, key: &str) -> bool {
        let ok = self
            .data
            .update_with(key, |val| {
                if val.is_expired() || val.expires_ms == 0 {
                    return false;
                }
                val.expires_ms = 0;
                true
            })
            .unwrap_or(false);
        if ok {
            self.sub_ttl();
        }
        ok
    }

    pub fn ttl(&self, key: &str) -> Option<Duration> {
        let data = self.data.get_ref(key)?;
        if data.is_expired() {
            return None;
        }
        data.ttl_ms().map(Duration::from_millis)
    }

    pub fn pttl(&self, key: &str) -> Option<Duration> {
        self.ttl(key)
    }

    pub fn ttl_value_ms(&self, key: &str) -> i64 {
        let data = match self.data.get_ref(key) {
            Some(data) => data,
            None => return -2,
        };
        if data.is_expired() {
            return -2;
        }
        data.ttl_ms()
            .map_or(-1, |ms| ms.min(i64::MAX as u64) as i64)
    }

    pub fn rename(&self, old_key: &str, new_key: &str) -> bool {
        if old_key == new_key {
            return self.exists(old_key);
        }
        match self.data.remove(old_key) {
            Some(entry) if !entry.is_expired() => {
                self.data.insert(new_key.to_string(), entry);
                true
            }
            _ => false,
        }
    }

    pub fn renamenx(&self, old_key: &str, new_key: &str) -> bool {
        if old_key == new_key {
            return self.exists(old_key);
        }
        match self.data.remove(old_key) {
            Some(entry) if !entry.is_expired() => {
                if self
                    .data
                    .insert_if_absent(new_key.to_string(), entry.clone())
                {
                    true
                } else {
                    self.data.insert(old_key.to_string(), entry);
                    false
                }
            }
            _ => false,
        }
    }

    pub fn copy(&self, src: &str, dst: &str, replace: bool) -> bool {
        let vref = match self.data.get_ref(src) {
            Some(e) if !e.is_expired() => e,
            _ => return false,
        };
        let entry: crate::storage::value::StoreValue = (*vref).clone();
        drop(vref);
        if replace {
            self.data.insert(dst.to_string(), entry);
            true
        } else {
            self.data.insert_if_absent(dst.to_string(), entry)
        }
    }

    pub fn randomkey(&self) -> Option<String> {
        let shard_count = self.data.shard_count();
        if shard_count == 0 || self.data.is_empty() {
            return None;
        }

        let now = now_ms();
        let stack_entropy = &now as *const u64 as u64;
        let mut seed = now ^ 0x517cc1b727220a95 ^ stack_entropy;
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;

        let shard_start = (seed as usize) % shard_count;
        let slot_seed = {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };

        for i in 0..shard_count {
            let shard_idx = (shard_start + i) % shard_count;
            let slot_count = self.data.shard_slot_count(shard_idx);
            if slot_count == 0 {
                continue;
            }
            let start = (slot_seed as usize) % slot_count;
            for offset in 0..slot_count {
                let slot_idx = (start + offset) % slot_count;
                if let Some((key, val)) = self.data.peek_slot(shard_idx, slot_idx)
                    && !val.is_expired()
                {
                    return Some(key);
                }
            }
        }
        None
    }

    pub fn keys_matching(&self, pattern: &str) -> Vec<String> {
        let mut keys = Vec::new();
        self.data.for_each(|key, val| {
            if !val.is_expired() && glob_match(pattern, key) {
                keys.push(key.to_string());
            }
        });
        keys
    }
}
