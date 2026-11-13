use arc_swap::ArcSwap;
use smallvec::SmallVec;
use std::hash::BuildHasher;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use super::frame::{
   encode_batch_bytes as encode_batch, encode_message_bytes as encode_message,
   encode_pmessage_batch, encode_pmessage_bytes as encode_pmessage,
};
use super::slot::{FanEntry, PATTERN_FLAG, SubSlot, WorkerNotifier};
use crate::utils::util::glob_match_bytes;
use foldhash::fast::RandomState;

static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);
fn next_generation() -> u64 {
   NEXT_GENERATION
      .try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
         value.checked_add(1).filter(|next| *next < PATTERN_FLAG)
      })
      .expect("pub/sub generations exhausted")
}

#[derive(Clone)]
enum Refs<T> {
   Empty,
   One(T),
   Many(Arc<[T]>),
}
impl<T> Deref for Refs<T> {
   type Target = [T];
   fn deref(&self) -> &[T] {
      match self {
         Self::Empty => &[],
         Self::One(value) => std::slice::from_ref(value),
         Self::Many(values) => values,
      }
   }
}
impl<T> Refs<T> {
   fn from_vec(mut values: Vec<T>) -> Self {
      match values.len() {
         0 => Self::Empty,
         1 => Self::One(values.pop().unwrap()),
         _ => Self::Many(values.into()),
      }
   }
}

fn extract_workers(slots: &[Arc<SubSlot>]) -> Refs<Arc<WorkerNotifier>> {
   let mut reps: [Option<Arc<WorkerNotifier>>; 64] = [const { None }; 64];
   let mut overflow = Vec::new();
   for slot in slots {
      let notifier = slot.notifier();
      let index = notifier.worker_index();
      if index < 64 {
         if reps[index].is_none() {
            reps[index] = Some(Arc::clone(notifier));
         }
      } else if !overflow.iter().any(|n| Arc::ptr_eq(n, notifier)) {
         overflow.push(Arc::clone(notifier));
      }
   }
   let count = reps.iter().flatten().count() + overflow.len();
   if count == 1 {
      return Refs::One(
         reps
            .into_iter()
            .flatten()
            .next()
            .or_else(|| overflow.pop())
            .unwrap(),
      );
   }
   let mut workers = Vec::with_capacity(count);
   workers.extend(reps.into_iter().flatten());
   workers.extend(overflow);
   Refs::from_vec(workers)
}

fn fan_out(channel: &ChannelData, generation: u64, frame: Arc<[u8]>) {
   let workers = &*channel.workers;
   if let Some((last, earlier)) = workers.split_last() {
      for worker in earlier {
         worker.notify_fanout(FanEntry::new(
            Arc::clone(&channel.name),
            Arc::clone(&frame),
            generation,
            false,
         ));
      }
      last.notify_fanout(FanEntry::new(
         Arc::clone(&channel.name),
         frame,
         generation,
         false,
      ));
   }
}

fn fan_out_pattern(pattern: &PatternEntry, generation: u64, frame: Arc<[u8]>) {
   let mut seen = 0u64;
   let mut last: Option<&Arc<WorkerNotifier>> = None;
   for (index, slot) in pattern.slots.iter().enumerate() {
      let notifier = slot.notifier();
      let worker = notifier.worker_index();
      if worker < 64 {
         let bit = 1u64 << worker;
         if seen & bit != 0 {
            continue;
         }
         seen |= bit;
      } else if pattern.slots[..index]
         .iter()
         .any(|slot| Arc::ptr_eq(slot.notifier(), notifier))
      {
         continue;
      }
      if let Some(previous) = last.replace(notifier) {
         previous.notify_fanout(FanEntry::new(
            Arc::clone(&pattern.pattern),
            Arc::clone(&frame),
            generation,
            true,
         ));
      }
   }
   if let Some(last) = last {
      last.notify_fanout(FanEntry::new(
         Arc::clone(&pattern.pattern),
         frame,
         generation,
         true,
      ));
   }
}

#[derive(Clone)]
struct ChannelData {
   name: Arc<str>,
   slots: Refs<Arc<SubSlot>>,
   workers: Refs<Arc<WorkerNotifier>>,
}
struct ChannelSnapshot {
   generation: u64,
   channels: Vec<ChannelData>,
}
impl ChannelSnapshot {
   fn locate(&self, channel: &str) -> Result<usize, usize> {
      if self.channels.len() <= 8 {
         self
            .channels
            .iter()
            .position(|ch| ch.name.as_ref() == channel)
            .ok_or(self.channels.len())
      } else {
         self
            .channels
            .binary_search_by(|ch| ch.name.as_ref().cmp(channel))
      }
   }
   fn find(&self, channel: &str) -> Option<&ChannelData> {
      self.locate(channel).ok().map(|i| &self.channels[i])
   }
}

const CHANNEL_SHARDS: usize = 64;
struct ChannelShard {
   snapshot: ArcSwap<ChannelSnapshot>,
   mu: std::sync::Mutex<()>,
}
impl ChannelShard {
   fn new() -> Self {
      Self {
         snapshot: ArcSwap::from_pointee(ChannelSnapshot {
            generation: 0,
            channels: Vec::new(),
         }),
         mu: std::sync::Mutex::new(()),
      }
   }
   fn publish(&self, channel: &str, frame: impl FnOnce() -> Arc<[u8]>) -> usize {
      let snapshot = self.snapshot.load();
      let Some(ch) = snapshot.find(channel) else {
         return 0;
      };
      let count = ch.slots.len();
      if count != 0 {
         fan_out(ch, snapshot.generation, frame());
      }
      count
   }
   fn subscribe(&self, channel: &str, slot: Arc<SubSlot>) -> Arc<str> {
      let _lock = self.mu.lock().unwrap_or_else(|e| e.into_inner());
      let old = self.snapshot.load_full();
      let index = old.locate(channel);
      if let Ok(index) = index
         && old.channels[index]
            .slots
            .iter()
            .any(|existing| Arc::ptr_eq(existing, &slot))
      {
         return Arc::clone(&old.channels[index].name);
      }
      let generation = next_generation();
      let mut channels = Vec::with_capacity(old.channels.len() + usize::from(index.is_err()));
      channels.extend(old.channels.iter().cloned());
      let name;
      match index {
         Ok(index) => {
            let ch = &mut channels[index];
            let mut slots = Vec::with_capacity(ch.slots.len() + 1);
            slots.extend(ch.slots.iter().cloned());
            slots.push(Arc::clone(&slot));
            if !ch
               .workers
               .iter()
               .any(|worker| Arc::ptr_eq(worker, slot.notifier()))
            {
               ch.workers = extract_workers(&slots);
            }
            ch.slots = Refs::from_vec(slots);
            name = Arc::clone(&ch.name);
         }
         Err(index) => {
            name = Arc::from(channel);
            channels.insert(
               index,
               ChannelData {
                  name: Arc::clone(&name),
                  slots: Refs::One(Arc::clone(&slot)),
                  workers: Refs::One(Arc::clone(slot.notifier())),
               },
            );
            if old.channels.len() == 8 {
               channels.sort_unstable_by(|a, b| a.name.cmp(&b.name));
            }
         }
      }
      slot
         .notifier()
         .register_subscription(Arc::clone(&name), slot.token, generation, false);
      self.snapshot.store(Arc::new(ChannelSnapshot {
         generation,
         channels,
      }));
      name
   }
   fn unsubscribe(&self, channel: &str, slot: &Arc<SubSlot>) {
      let _lock = self.mu.lock().unwrap_or_else(|e| e.into_inner());
      let old = self.snapshot.load_full();
      let Ok(index) = old.locate(channel) else {
         return;
      };
      let ch = &old.channels[index];
      if !ch.slots.iter().any(|s| Arc::ptr_eq(s, slot)) {
         return;
      }
      let slots: Vec<_> = ch
         .slots
         .iter()
         .filter(|s| !Arc::ptr_eq(s, slot))
         .cloned()
         .collect();
      let mut channels = old.channels.clone();
      if slots.is_empty() {
         channels.remove(index);
         if channels.is_empty() {
            channels = Vec::new();
         }
      } else {
         channels[index].workers = extract_workers(&slots);
         channels[index].slots = Refs::from_vec(slots);
      }
      slot
         .notifier()
         .unregister_subscription(channel, slot.token, false);
      self.snapshot.store(Arc::new(ChannelSnapshot {
         generation: next_generation(),
         channels,
      }));
   }
   fn active_channels(&self, pattern: Option<&str>) -> Vec<String> {
      self
         .snapshot
         .load()
         .channels
         .iter()
         .filter(|ch| {
            !ch.slots.is_empty()
               && pattern.is_none_or(|p| glob_match_bytes(p.as_bytes(), ch.name.as_bytes()))
         })
         .map(|ch| ch.name.to_string())
         .collect()
   }
   fn count_for(&self, channel: &str) -> usize {
      self
         .snapshot
         .load()
         .find(channel)
         .map_or(0, |ch| ch.slots.len())
   }
}

struct PatternEntry {
   pattern: Arc<str>,
   slots: SmallVec<[Arc<SubSlot>; 1]>,
}
struct PatternIndex {
   buckets: [Vec<PatternEntry>; 256],
   wildcard: Vec<PatternEntry>,
   generation: u64,
   total: usize,
}
impl PatternIndex {
   fn new() -> Self {
      Self {
         buckets: std::array::from_fn(|_| Vec::new()),
         wildcard: Vec::new(),
         generation: 0,
         total: 0,
      }
   }
   fn bucket_mut(&mut self, pattern: &str) -> &mut Vec<PatternEntry> {
      match pattern.as_bytes().first().copied() {
         Some(b'*' | b'?' | b'[') | None => &mut self.wildcard,
         Some(byte) => &mut self.buckets[byte as usize],
      }
   }
   fn add(&mut self, pattern: &str, slot: Arc<SubSlot>) -> Arc<str> {
      let generation = next_generation();
      let bucket = self.bucket_mut(pattern);
      let index = bucket.binary_search_by(|entry| entry.pattern.as_ref().cmp(pattern));
      let name;
      match index {
         Ok(index) => {
            let entry = &mut bucket[index];
            if entry.slots.iter().any(|s| Arc::ptr_eq(s, &slot)) {
               return Arc::clone(&entry.pattern);
            }
            entry.slots.push(Arc::clone(&slot));
            name = Arc::clone(&entry.pattern);
         }
         Err(index) => {
            name = Arc::from(pattern);
            bucket.insert(
               index,
               PatternEntry {
                  pattern: Arc::clone(&name),
                  slots: smallvec::smallvec![Arc::clone(&slot)],
               },
            );
            self.total += 1;
         }
      }
      self.generation = generation;
      slot
         .notifier()
         .register_subscription(Arc::clone(&name), slot.token, generation, true);
      name
   }
   fn remove(&mut self, pattern: &str, slot: &Arc<SubSlot>) {
      let bucket = self.bucket_mut(pattern);
      let Ok(index) = bucket.binary_search_by(|entry| entry.pattern.as_ref().cmp(pattern)) else {
         return;
      };
      if !bucket[index].slots.iter().any(|s| Arc::ptr_eq(s, slot)) {
         return;
      }
      bucket[index].slots.retain(|s| !Arc::ptr_eq(s, slot));
      let removed = bucket[index].slots.is_empty();
      if removed {
         bucket.remove(index);
      }
      self.total -= usize::from(removed);
      self.generation = next_generation();
      slot
         .notifier()
         .unregister_subscription(pattern, slot.token, true);
   }
   fn matching<'a>(&'a self, channel: &str) -> SmallVec<[&'a PatternEntry; 4]> {
      let mut matches = SmallVec::new();
      let first = channel.as_bytes().first().copied();
      let bucket = first.map_or(&[][..], |b| &self.buckets[b as usize]);
      for entry in self.wildcard.iter().chain(bucket) {
         if glob_match_bytes(entry.pattern.as_bytes(), channel.as_bytes()) {
            matches.push(entry);
         }
      }
      matches
   }
}

pub struct PubSub {
   shards: Box<[ChannelShard]>,
   patterns: std::sync::RwLock<Option<Box<PatternIndex>>>,
   pattern_count: AtomicUsize,
   hasher: RandomState,
}
impl Default for PubSub {
   fn default() -> Self {
      Self::new()
   }
}
impl PubSub {
   pub fn new() -> Self {
      Self {
         shards: (0..CHANNEL_SHARDS)
            .map(|_| ChannelShard::new())
            .collect::<Vec<_>>()
            .into_boxed_slice(),
         patterns: std::sync::RwLock::new(None),
         pattern_count: AtomicUsize::new(0),
         hasher: RandomState::default(),
      }
   }
   fn shard_for(&self, channel: &str) -> &ChannelShard {
      &self.shards[self.hasher.hash_one(channel) as usize % CHANNEL_SHARDS]
   }
   pub fn subscribe(&self, channel: &str, slot: Arc<SubSlot>) {
      self.subscribe_shared(channel, slot);
   }
   pub(crate) fn subscribe_shared(&self, channel: &str, slot: Arc<SubSlot>) -> Arc<str> {
      self.shard_for(channel).subscribe(channel, slot)
   }
   pub fn unsubscribe(&self, channel: &str, slot: &Arc<SubSlot>) {
      self.shard_for(channel).unsubscribe(channel, slot);
   }
   pub fn psubscribe(&self, pattern: &str, slot: Arc<SubSlot>) {
      self.psubscribe_shared(pattern, slot);
   }
   pub(crate) fn psubscribe_shared(&self, pattern: &str, slot: Arc<SubSlot>) -> Arc<str> {
      let mut guard = self.patterns.write().unwrap_or_else(|e| e.into_inner());
      let index = guard.get_or_insert_with(|| Box::new(PatternIndex::new()));
      let name = index.add(pattern, slot);
      self.pattern_count.store(index.total, Ordering::Release);
      name
   }
   pub fn punsubscribe(&self, pattern: &str, slot: &Arc<SubSlot>) {
      let mut guard = self.patterns.write().unwrap_or_else(|e| e.into_inner());
      if let Some(index) = guard.as_deref_mut() {
         index.remove(pattern, slot);
         self.pattern_count.store(index.total, Ordering::Release);
         if index.total == 0 {
            *guard = None;
         }
      }
   }
   #[inline]
   pub fn publish(&self, channel: &str, message: &str) -> usize {
      self.publish_batch(channel, &[message])
   }
   pub fn publish_batch(&self, channel: &str, messages: &[&str]) -> usize {
      self.publish_impl(channel, messages)
   }
   pub fn publish_bytes(&self, channel: &str, message: &[u8]) -> usize {
      self.publish_batch_bytes(channel, &[message])
   }
   pub fn publish_batch_bytes(&self, channel: &str, messages: &[&[u8]]) -> usize {
      self.publish_impl(channel, messages)
   }
   fn publish_impl<M: AsRef<[u8]>>(&self, channel: &str, messages: &[M]) -> usize {
      if messages.is_empty() {
         return 0;
      }
      let shard = self.shard_for(channel);
      if self.pattern_count.load(Ordering::Relaxed) == 0 {
         return shard.publish(channel, || encode_batch(channel, messages));
      }
      let patterns = self.patterns.read().unwrap_or_else(|e| e.into_inner());
      let Some(patterns) = patterns.as_deref() else {
         return shard.publish(channel, || encode_batch(channel, messages));
      };
      let matching = patterns.matching(channel);
      let snapshot = shard.snapshot.load();
      let direct = snapshot.find(channel);
      let direct_count = direct.map_or(0, |ch| ch.slots.len());
      if matching.is_empty() {
         if let Some(ch) = direct {
            fan_out(ch, snapshot.generation, encode_batch(channel, messages));
         }
         return direct_count;
      }
      let count = direct_count
         + matching
            .iter()
            .map(|entry| entry.slots.len())
            .sum::<usize>();
      if direct_count == 0 && matching.len() == 1 {
         let entry = matching[0];
         fan_out_pattern(
            entry,
            patterns.generation,
            encode_pmessage_batch(&entry.pattern, channel, messages),
         );
         return count;
      }
      // One lock covers the batch; both delivery kinds use the same worker queue.
      for message in messages {
         if let Some(ch) = direct {
            fan_out(
               ch,
               snapshot.generation,
               encode_message(channel, message.as_ref()),
            );
         }
         for entry in &matching {
            fan_out_pattern(
               entry,
               patterns.generation,
               encode_pmessage(&entry.pattern, channel, message.as_ref()),
            );
         }
      }
      count
   }
   pub fn active_channels(&self, pattern: Option<&str>) -> Vec<String> {
      self
         .shards
         .iter()
         .flat_map(|shard| shard.active_channels(pattern))
         .collect()
   }
   pub fn numsub(&self, channels: &[&str]) -> Vec<(String, usize)> {
      channels
         .iter()
         .map(|channel| {
            (
               channel.to_string(),
               self.shard_for(channel).count_for(channel),
            )
         })
         .collect()
   }
   pub fn numpat(&self) -> usize {
      self.pattern_count.load(Ordering::Acquire)
   }
}
