use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};

use foldhash::fast::RandomState;
use std::hash::BuildHasher;

use crate::utils::util::glob_match_bytes;

use super::frame::{encode_message, encode_pmessage};
use super::slot::{FanEntry, SubSlot, WorkerNotifier};

/// Route one channel's frame to every worker hosting at least one subscriber.
///
/// One queue entry per worker instead of one per subscriber: the publishing
/// worker used to perform every subscriber push itself, which scaled publish
/// cost linearly with subscriber count even though the recipients are spread
/// across workers. Each target worker resolves its own subscribers locally
/// and copies the frame bytes out — no per-subscriber atomics on the
/// publisher.
///
/// Pattern subscribers stay on the per-slot path: their frames carry a
/// per-pattern payload, and pattern matching is publish-time work.
/// Above this many subscribers per distinct worker, grouping the fan-out per
/// worker beats pushing to each subscriber's queue. Measured break-even on
/// 12 workers: per-slot wins through ~4 subscribers/worker, ties at 4-8, and
/// grouping is ~26% faster at 8+. The margin absorbs machine variance.
const FANOUT_GROUP_RATIO: usize = 8;

fn fan_out(ch: &ChannelData, encode: &dyn Fn() -> Arc<[u8]>) {
    // Distinct workers among the channel's subscribers. Worker indices fit
    // in a 64-bit mask on every supported machine; beyond that, fall back
    // to a linear scan (still correct, just slower). Both live on the stack:
    // this runs once per publish, and a heap allocation here would cost
    // more than the queue pushes it saves.
    let mut reps: [Option<&Arc<WorkerNotifier>>; 64] = [const { None }; 64];
    let mut overflow: Vec<&Arc<WorkerNotifier>> = Vec::new();
    for slot in &ch.slots {
        let notifier = slot.notifier();
        let idx = notifier.worker_index();
        if idx < 64 {
            if reps[idx].is_none() {
                reps[idx] = Some(notifier);
            }
        } else if !overflow.iter().any(|&n| Arc::ptr_eq(n, notifier)) {
            overflow.push(notifier);
        }
    }
    let distinct = reps.iter().flatten().count() + overflow.len();

    if ch.slots.len() <= FANOUT_GROUP_RATIO * distinct {
        // Small fan-out relative to the worker count: a queue push per
        // subscriber is already the cheapest route, and the per-worker
        // indirection would only add a hop.
        let frame = encode();
        for slot in &ch.slots {
            slot.push(Arc::clone(&frame));
        }
        return;
    }

    let frame = encode();
    for notifier in reps.iter().flatten() {
        notifier.notify_fanout(FanEntry {
            channel: Arc::clone(&ch.name),
            frame: Arc::clone(&frame),
        });
    }
    for notifier in overflow {
        notifier.notify_fanout(FanEntry {
            channel: Arc::clone(&ch.name),
            frame: Arc::clone(&frame),
        });
    }
}

struct PatternEntry {
    pattern: String,
    slot: Arc<SubSlot>,
}

const CHANNEL_SHARDS: usize = 64;

unsafe fn drop_snapshot_box(ptr: *mut u8) {
    unsafe { drop(Box::from_raw(ptr.cast::<Snapshot>())) };
}

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

    /// Lock-free snapshot load. The boxed slot is EBR-retired on swap, so the
    /// Arc stays alive while this guard is held.
    #[inline(always)]
    fn load_snapshot(&self) -> Snapshot {
        let _guard = customhash::pin();
        let ptr = self.snapshot.load(Ordering::Acquire);
        Arc::clone(unsafe { &*ptr })
    }

    /// Borrow the snapshot under the caller's EBR pin, without touching the
    /// Arc refcount.
    ///
    /// Retiring hands the boxed Arc to EBR, so the pointed-to snapshot
    /// outlives any pin held across this borrow. The hot publish path used to
    /// pay a contended clone-and-drop pair (two refcount RMWs on one cache
    /// line shared by every publisher thread) for a guard it already held.
    #[inline(always)]
    fn with_snapshot<R>(&self, f: impl FnOnce(&[ChannelData]) -> R) -> R {
        let _guard = customhash::pin();
        let ptr = self.snapshot.load(Ordering::Acquire);
        let snap = unsafe { &*ptr };
        f(&snap[..])
    }

    #[inline(always)]
    fn load_snapshot_locked(&self) -> Snapshot {
        let ptr = self.snapshot.load(Ordering::Acquire);
        Arc::clone(unsafe { &*ptr })
    }

    #[inline(always)]
    fn store_snapshot_locked(&self, new_snap: Snapshot) {
        let new_ptr = Box::into_raw(Box::new(new_snap));
        let old_ptr = self.snapshot.swap(new_ptr, Ordering::AcqRel);
        unsafe { customhash::retire_raw(old_ptr.cast::<u8>(), drop_snapshot_box) };
    }

    #[inline(always)]
    pub fn publish(&self, channel: &str, frame: &dyn Fn() -> Arc<[u8]>) -> usize {
        let mut n = 0;
        self.with_snapshot(|snap| {
            for ch in snap {
                if ch.name.as_ref() == channel {
                    n = ch.slots.len();
                    if n != 0 {
                        fan_out(ch, frame);
                    }
                    return;
                }
            }
        });
        n
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

        self.store_snapshot_locked(Arc::new(new_vec));
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

        self.store_snapshot_locked(Arc::new(new_vec));
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

    fn count_for(&self, channel: &str) -> usize {
        let snap = self.load_snapshot();
        for ch in snap.iter() {
            if ch.name.as_ref() == channel {
                return ch.slots.len();
            }
        }
        0
    }
}

// Final teardown frees the boxed snapshot directly: no concurrent readers can
// exist once the whole registry is dropping.
impl Drop for ChannelShard {
    fn drop(&mut self) {
        let ptr = *self.snapshot.get_mut();
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

        count += self
            .shard_for(channel)
            .publish(channel, &|| encode_message(channel, message));

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
