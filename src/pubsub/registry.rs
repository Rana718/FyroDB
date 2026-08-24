use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};

use foldhash::fast::RandomState;
use std::hash::BuildHasher;

use crate::utils::util::glob_match_bytes;

use super::frame::{encode_message, encode_pmessage};
use super::slot::SubSlot;

struct PatternEntry {
    pattern: String,
    slot: Arc<SubSlot>,
}

const CHANNEL_SHARDS: usize = 64;

struct ChannelData {
    name: Arc<str>,
    slots: Vec<Arc<SubSlot>>,
}

type Snapshot = Arc<Vec<ChannelData>>;

struct ChannelShard {
    snapshot: AtomicPtr<Snapshot>,
    mu: std::sync::Mutex<()>,
}

unsafe impl Send for ChannelShard {}
unsafe impl Sync for ChannelShard {}

impl ChannelShard {
    fn new() -> Self {
        let snap: Snapshot = Arc::new(Vec::new());
        Self {
            snapshot: AtomicPtr::new(Box::into_raw(Box::new(snap))),
            mu: std::sync::Mutex::new(()),
        }
    }

    /// Load the current Arc snapshot.
    #[inline(always)]
    fn load_snapshot(&self) -> Snapshot {
        // The pointer owns an Arc box that writers replace and immediately
        // reclaim. Serialize the load+clone with replacement so a publisher
        // can never clone from a freed box. The lock is held only for the
        // atomic pointer read and Arc increment; iteration happens lock-free.
        let _lock = self.mu.lock().unwrap_or_else(|e| e.into_inner());
        self.load_snapshot_locked()
    }

    #[inline(always)]
    fn load_snapshot_locked(&self) -> Snapshot {
        let ptr = self.snapshot.load(Ordering::Acquire);
        Arc::clone(unsafe { &*ptr })
    }

    #[inline(always)]
    fn publish(&self, channel: &str, frame: &Arc<[u8]>) -> usize {
        let snap = self.load_snapshot();
        for ch in snap.iter() {
            if ch.name.as_ref() == channel {
                let n = ch.slots.len();
                for slot in &ch.slots {
                    slot.push(Arc::clone(frame));
                }
                return n;
            }
        }
        0
    }

    fn subscribe(&self, channel: &str, slot: Arc<SubSlot>) {
        let _lock = self.mu.lock().unwrap_or_else(|e| e.into_inner());
        let old_snap = self.load_snapshot_locked();

        let mut new_vec: Vec<ChannelData> = Vec::with_capacity(old_snap.len() + 1);
        let mut found = false;
        for ch in old_snap.iter() {
            if ch.name.as_ref() == channel {
                let mut new_slots = ch.slots.clone();
                new_slots.push(slot.clone());
                new_vec.push(ChannelData {
                    name: ch.name.clone(),
                    slots: new_slots,
                });
                found = true;
            } else {
                new_vec.push(ChannelData {
                    name: ch.name.clone(),
                    slots: ch.slots.clone(),
                });
            }
        }
        if !found {
            new_vec.push(ChannelData {
                name: Arc::from(channel),
                slots: vec![slot],
            });
        }

        let new_snap: Snapshot = Arc::new(new_vec);
        let new_ptr = Box::into_raw(Box::new(new_snap));
        let old_ptr = self.snapshot.swap(new_ptr, Ordering::AcqRel);
        unsafe { drop(Box::from_raw(old_ptr)) };
    }

    fn unsubscribe(&self, channel: &str, slot: &Arc<SubSlot>) {
        let _lock = self.mu.lock().unwrap_or_else(|e| e.into_inner());
        let old_snap = self.load_snapshot_locked();

        let mut new_vec: Vec<ChannelData> = Vec::with_capacity(old_snap.len());
        for ch in old_snap.iter() {
            if ch.name.as_ref() == channel {
                let new_slots: Vec<Arc<SubSlot>> = ch
                    .slots
                    .iter()
                    .filter(|s| !Arc::ptr_eq(s, slot))
                    .cloned()
                    .collect();
                if !new_slots.is_empty() {
                    new_vec.push(ChannelData {
                        name: ch.name.clone(),
                        slots: new_slots,
                    });
                }
            } else {
                new_vec.push(ChannelData {
                    name: ch.name.clone(),
                    slots: ch.slots.clone(),
                });
            }
        }

        let new_snap: Snapshot = Arc::new(new_vec);
        let new_ptr = Box::into_raw(Box::new(new_snap));
        let old_ptr = self.snapshot.swap(new_ptr, Ordering::AcqRel);
        unsafe { drop(Box::from_raw(old_ptr)) };
    }

    fn count_for(&self, channel: &str) -> usize {
        let snap = self.load_snapshot();
        for ch in snap.iter() {
            if ch.name.as_ref() == channel {
                return ch.slots.len();
            }
        }
        0
    }

    fn active_channels(&self, pattern: Option<&str>) -> Vec<String> {
        let snap = self.load_snapshot();
        let mut result = Vec::new();
        for ch in snap.iter() {
            if !ch.slots.is_empty()
                && pattern.is_none_or(|p| glob_match_bytes(p.as_bytes(), ch.name.as_bytes()))
            {
                result.push(ch.name.to_string());
            }
        }
        result
    }
}

impl Drop for ChannelShard {
    fn drop(&mut self) {
        let ptr = self.snapshot.load(Ordering::Relaxed);
        if !ptr.is_null() {
            unsafe { drop(Box::from_raw(ptr)) };
        }
    }
}

pub struct PubSub {
    shards: Box<[ChannelShard]>,
    patterns: std::sync::RwLock<PatternIndex>,
    hasher: RandomState,
}

struct PatternIndex {
    buckets: [Vec<PatternEntry>; 256],
    wildcard: Vec<PatternEntry>,
    total: usize,
}

impl PatternIndex {
    fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| Vec::new()),
            wildcard: Vec::new(),
            total: 0,
        }
    }

    fn add(&mut self, pattern: String, slot: Arc<SubSlot>) {
        let entry = PatternEntry { pattern, slot };
        let first = entry.pattern.as_bytes().first().copied();
        match first {
            Some(b'*') | Some(b'?') | Some(b'[') | None => self.wildcard.push(entry),
            Some(b) => self.buckets[b as usize].push(entry),
        }
        self.total += 1;
    }

    fn remove(&mut self, pattern: &str, slot: &Arc<SubSlot>) {
        let first = pattern.as_bytes().first().copied();
        let vec = match first {
            Some(b'*') | Some(b'?') | Some(b'[') | None => &mut self.wildcard,
            Some(b) => &mut self.buckets[b as usize],
        };
        let before = vec.len();
        vec.retain(|e| !(e.pattern == pattern && Arc::ptr_eq(&e.slot, slot)));
        if vec.len() < before {
            self.total -= before - vec.len();
        }
    }

    fn is_empty(&self) -> bool {
        self.total == 0
    }

    fn len(&self) -> usize {
        self.total
    }
}

impl Default for PubSub {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSub {
    pub fn new() -> Self {
        let shards = (0..CHANNEL_SHARDS)
            .map(|_| ChannelShard::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            shards,
            patterns: std::sync::RwLock::new(PatternIndex::new()),
            hasher: RandomState::default(),
        }
    }

    #[inline(always)]
    fn shard_for(&self, channel: &str) -> &ChannelShard {
        let h = self.hasher.hash_one(channel) as usize;
        &self.shards[h % CHANNEL_SHARDS]
    }

    pub fn subscribe(&self, channel: &str, slot: Arc<SubSlot>) {
        self.shard_for(channel).subscribe(channel, slot);
    }

    pub fn unsubscribe(&self, channel: &str, slot: &Arc<SubSlot>) {
        self.shard_for(channel).unsubscribe(channel, slot);
    }

    pub fn psubscribe(&self, pattern: &str, slot: Arc<SubSlot>) {
        let mut idx = self.patterns.write().unwrap_or_else(|e| e.into_inner());
        idx.add(pattern.to_string(), slot);
    }

    pub fn punsubscribe(&self, pattern: &str, slot: &Arc<SubSlot>) {
        let mut idx = self.patterns.write().unwrap_or_else(|e| e.into_inner());
        idx.remove(pattern, slot);
    }

    #[inline]
    pub fn publish(&self, channel: &str, message: &str) -> usize {
        let mut count = 0usize;

        let frame: Arc<[u8]> = encode_message(channel, message);
        count += self.shard_for(channel).publish(channel, &frame);

        let guard = self.patterns.read().unwrap_or_else(|e| e.into_inner());
        if !guard.is_empty() {
            let chan_b = channel.as_bytes();
            let check = |entry: &PatternEntry| {
                if glob_match_bytes(entry.pattern.as_bytes(), chan_b) {
                    let pframe: Arc<[u8]> = encode_pmessage(&entry.pattern, channel, message);
                    entry.slot.push(pframe);
                    true
                } else {
                    false
                }
            };

            for entry in &guard.wildcard {
                if check(entry) {
                    count += 1;
                }
            }
            if let Some(&first_byte) = chan_b.first() {
                for entry in &guard.buckets[first_byte as usize] {
                    if check(entry) {
                        count += 1;
                    }
                }
            }
        }

        count
    }

    pub fn active_channels(&self, pattern: Option<&str>) -> Vec<String> {
        let mut result = Vec::new();
        for shard in self.shards.iter() {
            result.extend(shard.active_channels(pattern));
        }
        result
    }

    pub fn numsub(&self, channels: &[&str]) -> Vec<(String, usize)> {
        channels
            .iter()
            .map(|&ch| {
                let n = self.shard_for(ch).count_for(ch);
                (ch.to_string(), n)
            })
            .collect()
    }

    pub fn numpat(&self) -> usize {
        self.patterns
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }
}
