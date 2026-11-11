mod collections;
mod json_value;
mod list;
mod quicklist;
mod small_str;
mod zset_data;

pub use collections::{HashInner, HashIter, SetInner, SetIter, SetMemberRef, canonical_i64};
pub use list::ListValue;
pub use quicklist::QuickList;
pub type ListInner = ListValue;
pub use json_value::JsonValue;
pub use small_str::SmallStr;
pub use zset_data::{ZEntry, ZSetData};

use foldhash::{HashMap, HashSet};
use std::collections::VecDeque;

#[derive(Clone)]
pub enum FyroDB {
   String(SmallStr),
   Hash(Box<HashInner>),
   List(Box<ListInner>),
   Set(Box<SetInner>),
   ZSet(Box<ZSetData>),
   Json(Box<str>),
   Stream(Box<crate::storage::stream::StreamData>),
}

impl FyroDB {
   pub fn compact_allocations(&mut self) {
      match self {
         FyroDB::Hash(inner) => match inner.as_mut() {
            HashInner::Compact(v) => v.shrink_to_fit(),
            HashInner::Full(m) => m.shrink_to_fit(),
         },
         FyroDB::List(inner) => inner.shrink_to_fit(),
         FyroDB::Set(inner) => match inner.as_mut() {
            SetInner::Integers(v) => v.shrink_to_fit(),
            SetInner::Compact(v) => v.shrink_to_fit(),
            SetInner::Full(v) => v.shrink_to_fit(),
         },
         FyroDB::ZSet(z) => z.shrink_to_fit(),
         FyroDB::Stream(s) => {
            s.groups.shrink_to_fit();
         }
         FyroDB::String(_) | FyroDB::Json(_) => {}
      }
   }

   pub fn type_name(&self) -> &'static str {
      match self {
         Self::String(_) => "string",
         Self::Hash(_) => "hash",
         Self::List(_) => "list",
         Self::Set(_) => "set",
         Self::ZSet(_) => "zset",
         Self::Json(_) => "ReJSON-RL",
         Self::Stream(_) => "stream",
      }
   }

   pub fn as_string(&self) -> Option<&SmallStr> {
      match self {
         Self::String(s) => Some(s),
         _ => None,
      }
   }

   pub fn as_string_mut(&mut self) -> Option<&mut SmallStr> {
      match self {
         Self::String(s) => Some(s),
         _ => None,
      }
   }

   pub fn as_hash(&self) -> Option<&HashInner> {
      match self {
         Self::Hash(h) => Some(h),
         _ => None,
      }
   }

   pub fn as_hash_mut(&mut self) -> Option<&mut HashInner> {
      match self {
         Self::Hash(h) => Some(h),
         _ => None,
      }
   }

   /// A drained list is kept allocated for queue reuse but is logically
   /// absent (Redis deletes a list when its last element is popped).
   #[inline]
   pub fn is_drained_collection(&self) -> bool {
      matches!(self, Self::List(l) if l.is_empty())
   }

   pub fn as_list(&self) -> Option<&ListInner> {
      match self {
         Self::List(l) => Some(l),
         _ => None,
      }
   }

   pub fn as_list_mut(&mut self) -> Option<&mut ListInner> {
      match self {
         Self::List(l) => Some(l),
         _ => None,
      }
   }

   pub fn as_set(&self) -> Option<&SetInner> {
      match self {
         Self::Set(s) => Some(s),
         _ => None,
      }
   }

   pub fn as_set_mut(&mut self) -> Option<&mut SetInner> {
      match self {
         Self::Set(s) => Some(s),
         _ => None,
      }
   }

   pub fn as_zset(&self) -> Option<&ZSetData> {
      match self {
         Self::ZSet(z) => Some(z),
         _ => None,
      }
   }

   pub fn as_zset_mut(&mut self) -> Option<&mut ZSetData> {
      match self {
         Self::ZSet(z) => Some(z),
         _ => None,
      }
   }

   pub fn as_json_str(&self) -> Option<&str> {
      match self {
         Self::Json(s) => Some(s),
         _ => None,
      }
   }

   pub fn parse_json(&self) -> Option<JsonValue> {
      match self {
         Self::Json(s) => JsonValue::parse(s),
         _ => None,
      }
   }

   pub fn set_json(&mut self, val: JsonValue) {
      *self = Self::Json(val.to_resp_string().into_boxed_str());
   }

   pub fn set_json_str(&mut self, s: String) {
      *self = Self::Json(s.into_boxed_str());
   }

   pub fn as_stream(&self) -> Option<&crate::storage::stream::StreamData> {
      match self {
         Self::Stream(s) => Some(s),
         _ => None,
      }
   }

   pub fn as_stream_mut(&mut self) -> Option<&mut crate::storage::stream::StreamData> {
      match self {
         Self::Stream(s) => Some(s),
         _ => None,
      }
   }

   pub fn mem_size(&self) -> usize {
      match self {
         Self::String(s) => s.len(),
         Self::Hash(h) => h.iter().map(|(k, v)| k.len() + v.len()).sum(),
         Self::List(l) => l.memory_bytes(),
         Self::Set(s) => s.iter().map(|m| m.len() + 24).sum(),
         Self::ZSet(z) => z.iter().map(|e| e.member.len() + 32).sum(),
         Self::Json(j) => j.len() + 16,
         Self::Stream(s) => s.memory_bytes(),
      }
   }
}

#[derive(Clone)]
pub struct StoreValue {
   pub value: FyroDB,
   #[cfg(feature = "expiration")]
   expires_ms: Option<std::num::NonZeroU64>,
}

static UNIX_MS_CACHE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[inline(always)]
pub fn now_ms() -> u64 {
   use std::time::{SystemTime, UNIX_EPOCH};
   SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap_or_default()
      .as_millis() as u64
}

#[inline]
pub fn tick_clock() {
   UNIX_MS_CACHE.store(now_ms(), std::sync::atomic::Ordering::Relaxed);
}

#[inline]
pub fn expiry_from_secs(secs: u64) -> Option<u64> {
   approx_now_ms().checked_add(secs.checked_mul(1000)?)
}

#[inline]
pub fn expiry_from_ms(ms: u64) -> Option<u64> {
   approx_now_ms().checked_add(ms)
}

#[inline]
pub fn expiry_from_unix_secs(unix_secs: u64) -> Option<u64> {
   unix_secs.checked_mul(1000)
}

#[inline(always)]
pub fn approx_now_ms() -> u64 {
   let cached = UNIX_MS_CACHE.load(std::sync::atomic::Ordering::Relaxed);
   if cached == 0 { now_ms() } else { cached }
}

impl StoreValue {
   pub fn compact_allocations(&mut self) {
      self.value.compact_allocations();
   }

   #[inline(always)]
   pub const fn new(value: FyroDB) -> Self {
      Self {
         value,
         #[cfg(feature = "expiration")]
         expires_ms: None,
      }
   }

   #[inline(always)]
   pub const fn new_with_expiry_opt(
      value: FyroDB,
      _expires_ms: Option<std::num::NonZeroU64>,
   ) -> Self {
      Self {
         value,
         #[cfg(feature = "expiration")]
         expires_ms: _expires_ms,
      }
   }

   #[inline(always)]
   pub fn new_with_expiry(value: FyroDB, _expires_ms: u64) -> Self {
      Self {
         value,
         #[cfg(feature = "expiration")]
         expires_ms: std::num::NonZeroU64::new(_expires_ms),
      }
   }

   #[inline(always)]
   pub fn string(s: String) -> Self {
      Self::string_small(SmallStr::from_string(s))
   }

   #[inline(always)]
   pub const fn string_small(s: SmallStr) -> Self {
      Self {
         value: FyroDB::String(s),
         #[cfg(feature = "expiration")]
         expires_ms: None,
      }
   }

   #[inline(always)]
   pub const fn string_small_with_expiry_opt(
      s: SmallStr,
      _expires_ms: Option<std::num::NonZeroU64>,
   ) -> Self {
      Self {
         value: FyroDB::String(s),
         #[cfg(feature = "expiration")]
         expires_ms: _expires_ms,
      }
   }

   #[inline(always)]
   pub fn string_small_with_expiry(s: SmallStr, _expire_ms: u64) -> Self {
      Self {
         value: FyroDB::String(s),
         #[cfg(feature = "expiration")]
         expires_ms: std::num::NonZeroU64::new(_expire_ms),
      }
   }

   #[inline(always)]
   pub fn string_str(s: &str) -> Self {
      Self::string_small(SmallStr::new(s))
   }

   #[inline]
   pub fn string_str_with_expiry(s: &str, expires_at: Option<std::time::Instant>) -> Self {
      let expires_ms = match expires_at {
         None => return Self::string_small(SmallStr::new(s)),
         Some(exp) => {
            let remaining = exp.saturating_duration_since(std::time::Instant::now());
            now_ms().saturating_add(remaining.as_millis() as u64)
         }
      };
      Self::string_small_with_expiry(SmallStr::new(s), expires_ms)
   }

   #[inline]
   pub fn string_with_expiry(s: String, expires_at: Option<std::time::Instant>) -> Self {
      let expires_ms = match expires_at {
         None => return Self::string_small(SmallStr::from_string(s)),
         Some(exp) => {
            let remaining = exp.saturating_duration_since(std::time::Instant::now());
            now_ms().saturating_add(remaining.as_millis() as u64)
         }
      };
      Self::string_small_with_expiry(SmallStr::from_string(s), expires_ms)
   }

   #[inline]
   pub fn hash(h: HashMap<String, String>) -> Self {
      let h = h
         .into_iter()
         .map(|(k, v)| (SmallStr::from_string(k), SmallStr::from_string(v)))
         .collect();
      Self::hash_inner(HashInner::Full(Box::new(h)))
   }

   #[inline]
   pub fn hash_with_expiry(h: HashMap<String, String>, expires_ms: u64) -> Self {
      let h = h
         .into_iter()
         .map(|(k, v)| (SmallStr::from_string(k), SmallStr::from_string(v)))
         .collect();
      Self::hash_inner_with_expiry(HashInner::Full(Box::new(h)), expires_ms)
   }

   #[inline(always)]
   pub fn hash_inner(h: HashInner) -> Self {
      Self {
         value: FyroDB::Hash(Box::new(h)),
         #[cfg(feature = "expiration")]
         expires_ms: None,
      }
   }

   #[inline(always)]
   pub fn hash_inner_with_expiry(h: HashInner, _expires_ms: u64) -> Self {
      Self {
         value: FyroDB::Hash(Box::new(h)),
         #[cfg(feature = "expiration")]
         expires_ms: std::num::NonZeroU64::new(_expires_ms),
      }
   }

   #[inline]
   pub fn list(l: VecDeque<String>) -> Self {
      Self::list_inner(ListValue::from_strs(l.iter().map(String::as_str)))
   }

   #[inline(always)]
   pub fn list_inner(l: ListInner) -> Self {
      Self {
         value: FyroDB::List(Box::new(l)),
         #[cfg(feature = "expiration")]
         expires_ms: None,
      }
   }

   #[inline(always)]
   pub fn list_inner_with_expiry(l: ListInner, _expires_ms: u64) -> Self {
      Self {
         value: FyroDB::List(Box::new(l)),
         #[cfg(feature = "expiration")]
         expires_ms: std::num::NonZeroU64::new(_expires_ms),
      }
   }

   #[inline]
   pub fn set(s: HashSet<String>) -> Self {
      Self::set_inner(SetInner::from_strings(s))
   }

   #[inline]
   pub fn set_with_expiry(s: HashSet<String>, expires_ms: u64) -> Self {
      Self::set_inner_with_expiry(SetInner::from_strings(s), expires_ms)
   }

   #[inline(always)]
   pub fn set_inner(s: SetInner) -> Self {
      Self {
         value: FyroDB::Set(Box::new(s)),
         #[cfg(feature = "expiration")]
         expires_ms: None,
      }
   }

   #[inline(always)]
   pub fn set_inner_with_expiry(s: SetInner, _expires_ms: u64) -> Self {
      Self {
         value: FyroDB::Set(Box::new(s)),
         #[cfg(feature = "expiration")]
         expires_ms: std::num::NonZeroU64::new(_expires_ms),
      }
   }

   #[inline(always)]
   pub fn zset(z: ZSetData) -> Self {
      Self {
         value: FyroDB::ZSet(Box::new(z)),
         #[cfg(feature = "expiration")]
         expires_ms: None,
      }
   }

   #[inline(always)]
   pub fn zset_with_expiry(z: ZSetData, _expires_ms: u64) -> Self {
      Self {
         value: FyroDB::ZSet(Box::new(z)),
         #[cfg(feature = "expiration")]
         expires_ms: std::num::NonZeroU64::new(_expires_ms),
      }
   }

   #[inline(always)]
   pub fn json(j: JsonValue) -> Self {
      Self {
         value: FyroDB::Json(j.to_resp_string().into_boxed_str()),
         #[cfg(feature = "expiration")]
         expires_ms: None,
      }
   }

   #[inline(always)]
   pub fn json_str(s: &str) -> Self {
      Self {
         value: FyroDB::Json(Box::from(s)),
         #[cfg(feature = "expiration")]
         expires_ms: None,
      }
   }

   #[inline(always)]
   pub fn json_raw(s: String) -> Self {
      Self {
         value: FyroDB::Json(s.into_boxed_str()),
         #[cfg(feature = "expiration")]
         expires_ms: None,
      }
   }

   #[inline(always)]
   pub fn json_raw_with_expiry(s: String, _expires_ms: u64) -> Self {
      Self {
         value: FyroDB::Json(s.into_boxed_str()),
         #[cfg(feature = "expiration")]
         expires_ms: std::num::NonZeroU64::new(_expires_ms),
      }
   }

   #[inline(always)]
   pub fn stream(s: crate::storage::stream::StreamData) -> Self {
      Self {
         value: FyroDB::Stream(Box::new(s)),
         #[cfg(feature = "expiration")]
         expires_ms: None,
      }
   }

   #[inline(always)]
   pub fn stream_with_expiry(s: crate::storage::stream::StreamData, _expires_ms: u64) -> Self {
      Self {
         value: FyroDB::Stream(Box::new(s)),
         #[cfg(feature = "expiration")]
         expires_ms: std::num::NonZeroU64::new(_expires_ms),
      }
   }

   #[inline(always)]
   #[cfg(feature = "expiration")]
   pub fn is_expired(&self) -> bool {
      match self.expires_ms {
         Some(expires_ms) => Self::is_expired_slow(expires_ms),
         None => false,
      }
   }
   #[cold]
   #[inline(never)]
   #[cfg(feature = "expiration")]
   fn is_expired_slow(expires_ms: std::num::NonZeroU64) -> bool {
      approx_now_ms() >= expires_ms.get()
   }
   #[inline(always)]
   #[cfg(not(feature = "expiration"))]
   pub const fn is_expired(&self) -> bool {
      false
   }

   /// Same test with a caller-supplied clock, so bulk scans pay one clock
   /// read instead of one per key.
   #[inline(always)]
   #[cfg(feature = "expiration")]
   pub fn is_expired_at(&self, now_ms: u64) -> bool {
      match self.expires_ms {
         Some(expires_ms) => now_ms >= expires_ms.get(),
         None => false,
      }
   }
   #[inline(always)]
   #[cfg(not(feature = "expiration"))]
   pub const fn is_expired_at(&self, _now_ms: u64) -> bool {
      false
   }

   #[inline(always)]
   #[cfg(feature = "expiration")]
   pub fn is_expired_precise(&self) -> bool {
      match self.expires_ms {
         Some(expires_ms) => Self::is_expired_precise_slow(expires_ms),
         None => false,
      }
   }
   #[cold]
   #[inline(never)]
   #[cfg(feature = "expiration")]
   fn is_expired_precise_slow(expires_ms: std::num::NonZeroU64) -> bool {
      now_ms() >= expires_ms.get()
   }
   #[inline(always)]
   #[cfg(not(feature = "expiration"))]
   pub const fn is_expired_precise(&self) -> bool {
      false
   }

   #[inline(always)]
   #[cfg(feature = "expiration")]
   pub fn ttl_ms(&self) -> Option<u64> {
      match self.expires_ms {
         Some(expires_ms) => Self::ttl_ms_slow(expires_ms),
         None => None,
      }
   }
   #[cold]
   #[inline(never)]
   #[cfg(feature = "expiration")]
   fn ttl_ms_slow(expires_ms: std::num::NonZeroU64) -> Option<u64> {
      let exp = expires_ms.get();
      let now = now_ms();
      if exp > now { Some(exp - now) } else { None }
   }
   #[inline(always)]
   #[cfg(not(feature = "expiration"))]
   pub const fn ttl_ms(&self) -> Option<u64> {
      None
   }

   #[inline(always)]
   #[cfg(feature = "expiration")]
   pub const fn expires_at(&self) -> Option<std::num::NonZeroU64> {
      self.expires_ms
   }
   #[inline(always)]
   #[cfg(not(feature = "expiration"))]
   pub const fn expires_at(&self) -> Option<std::num::NonZeroU64> {
      None
   }

   #[inline(always)]
   #[cfg(feature = "expiration")]
   pub const fn has_expiry(&self) -> bool {
      self.expires_ms.is_some()
   }
   #[inline(always)]
   #[cfg(not(feature = "expiration"))]
   pub const fn has_expiry(&self) -> bool {
      false
   }

   #[inline(always)]
   #[cfg(feature = "expiration")]
   pub const fn is_persistent(&self) -> bool {
      self.expires_ms.is_none()
   }
   #[inline(always)]
   #[cfg(not(feature = "expiration"))]
   pub const fn is_persistent(&self) -> bool {
      true
   }

   #[inline(always)]
   #[cfg(feature = "expiration")]
   pub fn expires_ms_u64(&self) -> u64 {
      match self.expires_ms {
         Some(v) => v.get(),
         None => 0,
      }
   }
   #[inline(always)]
   #[cfg(not(feature = "expiration"))]
   pub const fn expires_ms_u64(&self) -> u64 {
      0
   }

   #[inline(always)]
   #[cfg(feature = "expiration")]
   pub fn clear_expiry(&mut self) {
      self.expires_ms = None;
   }
   #[inline(always)]
   #[cfg(not(feature = "expiration"))]
   pub const fn clear_expiry(&mut self) {}

   #[inline(always)]
   #[cfg(feature = "expiration")]
   pub fn set_expires_ms_opt(&mut self, expires_ms: Option<std::num::NonZeroU64>) {
      self.expires_ms = expires_ms;
   }
   #[inline(always)]
   #[cfg(not(feature = "expiration"))]
   pub const fn set_expires_ms_opt(&mut self, _expires_ms: Option<std::num::NonZeroU64>) {}

   #[inline(always)]
   #[cfg(feature = "expiration")]
   pub fn set_expires_ms(&mut self, expires_ms: u64) {
      self.expires_ms = std::num::NonZeroU64::new(expires_ms);
   }
   #[inline(always)]
   #[cfg(not(feature = "expiration"))]
   pub const fn set_expires_ms(&mut self, _expires_ms: u64) {}

   #[inline(always)]
   #[cfg(feature = "expiration")]
   pub fn get_expires_ms(&self) -> Option<u64> {
      self.expires_ms.map(|v| v.get())
   }
   #[inline(always)]
   #[cfg(not(feature = "expiration"))]
   pub const fn get_expires_ms(&self) -> Option<u64> {
      None
   }
}

const _: () = assert!(std::mem::size_of::<SmallStr>() == 24);
const _: () = assert!(std::mem::size_of::<FyroDB>() == 24);
#[cfg(feature = "expiration")]
const _: () = assert!(std::mem::size_of::<StoreValue>() == 32);
#[cfg(not(feature = "expiration"))]
const _: () = assert!(std::mem::size_of::<StoreValue>() == 24);
#[cfg(feature = "expiration")]
const _: () = assert!(customhash::entry_size::<StoreValue>() == 56);
#[cfg(not(feature = "expiration"))]
const _: () = assert!(customhash::entry_size::<StoreValue>() == 48);
