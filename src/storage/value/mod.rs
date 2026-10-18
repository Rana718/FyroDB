mod collections;
mod json_value;
mod list;
mod quicklist;
mod small_str;
mod zset_data;

pub use collections::{HashInner, HashIter, SetInner, SetIter, SetMemberRef};
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
   pub expires_ms: u64,
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

/// Formats an integer into `buf`, returning the written slice; i64 never
/// needs more than 20 ASCII bytes.
#[inline]
pub fn write_int_to(buf: &mut [u8; 20], mut n: i64) -> &str {
   let mut i = buf.len();
   let neg = n < 0;
   if neg {
      let mut m = (n as i128).unsigned_abs();
      while m >= 10 {
         i -= 1;
         buf[i] = b'0' + (m % 10) as u8;
         m /= 10;
      }
      i -= 1;
      buf[i] = b'0' + m as u8;
      i -= 1;
      buf[i] = b'-';
   } else {
      while n >= 10 {
         i -= 1;
         buf[i] = b'0' + (n % 10) as u8;
         n /= 10;
      }
      i -= 1;
      buf[i] = b'0' + n as u8;
   }
   unsafe { std::str::from_utf8_unchecked(&buf[i..]) }
}

impl StoreValue {
   pub fn compact_allocations(&mut self) {
      self.value.compact_allocations();
   }
   #[inline]
   pub fn string(s: String) -> Self {
      Self {
         value: FyroDB::String(SmallStr::from_string(s)),
         expires_ms: 0,
      }
   }

   #[inline]
   pub fn string_small(s: SmallStr) -> Self {
      Self {
         value: FyroDB::String(s),
         expires_ms: 0,
      }
   }

   #[inline]
   pub fn string_with_expiry(s: String, expires_at: Option<std::time::Instant>) -> Self {
      let expires_ms = match expires_at {
         None => 0,
         Some(exp) => {
            let remaining = exp.saturating_duration_since(std::time::Instant::now());
            now_ms().saturating_add(remaining.as_millis() as u64)
         }
      };
      Self {
         value: FyroDB::String(SmallStr::from_string(s)),
         expires_ms,
      }
   }

   #[inline]
   pub fn hash(h: HashMap<String, String>) -> Self {
      let h = h
         .into_iter()
         .map(|(k, v)| (SmallStr::from_string(k), SmallStr::from_string(v)))
         .collect();
      Self {
         value: FyroDB::Hash(Box::new(HashInner::Full(Box::new(h)))),
         expires_ms: 0,
      }
   }

   #[inline]
   pub fn list(l: VecDeque<String>) -> Self {
      Self {
         value: FyroDB::List(Box::new(ListValue::from_strs(l.iter().map(String::as_str)))),
         expires_ms: 0,
      }
   }

   #[inline]
   pub fn set(s: HashSet<String>) -> Self {
      Self {
         value: FyroDB::Set(Box::new(SetInner::from_strings(s))),
         expires_ms: 0,
      }
   }

   #[inline]
   pub fn zset(z: ZSetData) -> Self {
      Self {
         value: FyroDB::ZSet(Box::new(z)),
         expires_ms: 0,
      }
   }

   #[inline]
   pub fn json(j: JsonValue) -> Self {
      Self {
         value: FyroDB::Json(j.to_resp_string().into_boxed_str()),
         expires_ms: 0,
      }
   }

   #[inline]
   pub fn json_str(s: &str) -> Self {
      Self {
         value: FyroDB::Json(Box::from(s)),
         expires_ms: 0,
      }
   }

   #[inline]
   pub fn json_raw(s: String) -> Self {
      Self {
         value: FyroDB::Json(s.into_boxed_str()),
         expires_ms: 0,
      }
   }

   #[inline]
   pub fn stream(s: crate::storage::stream::StreamData) -> Self {
      Self {
         value: FyroDB::Stream(Box::new(s)),
         expires_ms: 0,
      }
   }

   #[inline(always)]
   pub fn is_expired(&self) -> bool {
      self.expires_ms != 0 && approx_now_ms() >= self.expires_ms
   }

   #[inline]
   pub fn is_expired_precise(&self) -> bool {
      self.expires_ms != 0 && now_ms() >= self.expires_ms
   }

   #[inline]
   pub fn ttl_ms(&self) -> Option<u64> {
      if self.expires_ms == 0 {
         return None;
      }
      let now = now_ms();
      if now >= self.expires_ms {
         None
      } else {
         Some(self.expires_ms - now)
      }
   }
}

const _: () = assert!(std::mem::size_of::<SmallStr>() == 24);
const _: () = assert!(std::mem::size_of::<FyroDB>() == 24);
const _: () = assert!(std::mem::size_of::<StoreValue>() == 32);
const _: () = assert!(customhash::entry_size::<StoreValue>() == 64);
