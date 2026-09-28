use crate::storage::store::Store;
use crate::utils::util::glob_match;

impl Store {
    pub fn scan(&self, cursor: usize, pattern: Option<&str>, count: usize) -> (usize, Vec<String>) {
        let mut keys = Vec::with_capacity(count);
        let total_shards = self.shard_count();

        if total_shards == 0 {
            return (0, keys);
        }

        let (mut shard_idx, mut slot_idx) = if cursor == 0 {
            (0usize, 0usize)
        } else {
            ((cursor >> 20) & 0xFFF, cursor & 0xF_FFFF)
        };

        while shard_idx < total_shards {
            let shard_len = self.shard_slot_count(shard_idx);

            while slot_idx < shard_len {
                if let Some(Some(key)) = self.data.peek_slot_with(shard_idx, slot_idx, |k, v| {
                    (!v.is_expired() && pattern.is_none_or(|p| glob_match(p, k))).then(|| k.to_string())
                }) {
                    keys.push(key);
                }
                slot_idx += 1;

                if keys.len() >= count {
                    if slot_idx >= shard_len {
                        shard_idx += 1;
                        slot_idx = 0;
                    }
                    if shard_idx >= total_shards {
                        return (0, keys);
                    }
                    let next_cursor = (shard_idx << 20) | (slot_idx & 0xF_FFFF);
                    return (next_cursor, keys);
                }
            }

            shard_idx += 1;
            slot_idx = 0;
        }

        (0, keys)
    }

    #[inline]
    fn shard_count(&self) -> usize {
        self.data.shard_count()
    }

    #[inline]
    fn shard_slot_count(&self, shard: usize) -> usize {
        self.data.shard_slot_count(shard)
    }
}
