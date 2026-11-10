use crate::storage::{
   store::Store,
   value::{SmallStr, StoreValue},
};
use customhash::Full;

const MAX_STRING_BYTES: usize = 512 * 1024 * 1024;

fn lcs_last_row(a: &[u8], b: &[u8]) -> Vec<u16> {
   let n = b.len();
   let mut prev = vec![0u16; n + 1];
   let mut curr = vec![0u16; n + 1];
   for &ai in a {
      for j in 1..=n {
         curr[j] = if ai == b[j - 1] {
            prev[j - 1] + 1
         } else {
            prev[j].max(curr[j - 1])
         };
      }
      std::mem::swap(&mut prev, &mut curr);
      curr.fill(0);
   }
   prev
}

fn hirschberg(a: &[u8], b: &[u8], out: &mut Vec<u8>) {
   if a.is_empty() || b.is_empty() {
      return;
   }
   if a.len() == 1 {
      if b.contains(&a[0]) {
         out.push(a[0]);
      }
      return;
   }
   let mid = a.len() / 2;
   let row_top = lcs_last_row(&a[..mid], b);
   let a_bot: Vec<u8> = a[mid..].iter().copied().rev().collect();
   let b_rev: Vec<u8> = b.iter().copied().rev().collect();
   let row_bot = lcs_last_row(&a_bot, &b_rev);
   let n = b.len();
   let mut best_j = 0;
   let mut best_score = 0u16;
   for j in 0..=n {
      let score = row_top[j] + row_bot[n - j];
      if score >= best_score {
         best_score = score;
         best_j = j;
      }
   }
   hirschberg(&a[..mid], &b[..best_j], out);
   hirschberg(&a[mid..], &b[best_j..], out);
}

impl Store {
   pub fn set(&self, key: &str, value: StoreValue) {
      if value.get_expires_ms().is_some() {
         self.add_ttl();
      }
      self.data.insert_str(key, value);
   }

   pub fn try_set_value(&self, key: &str, value: StoreValue) -> Result<(), Full> {
      if value.get_expires_ms().is_some() {
         self.add_ttl();
      }
      self.data.try_set_str(key, value)?;
      Ok(())
   }

   #[inline(always)]
   pub fn set_string(&self, key: &str, value: &str, expires_ms: u64) {
      self.set_string_hinted(key, None, value, expires_ms);
   }

   #[inline(always)]
   pub fn set_string_hinted(&self, key: &str, hash: Option<u64>, value: &str, expires_ms: u64) {
      let (h, idx) = match hash {
         Some(h) => (h, self.data.shard_of(h)),
         None => self.data.locate_key(key),
      };
      let store_val = StoreValue::string_small_with_expiry(SmallStr::new(value), expires_ms);
      if self.data.insert_slot_fast_located(h, idx, key, store_val) {
         self.data.touched(key);
         if expires_ms != 0 {
            self.add_ttl();
         }
         return;
      }
      self.set_string_hinted_pinned(key, Some(h), value, expires_ms);
   }

   #[inline(always)]
   pub fn set_string_hinted_pinned(
      &self,
      key: &str,
      hash: Option<u64>,
      value: &str,
      expires_ms: u64,
   ) {
      let store_val = StoreValue::string_small_with_expiry(SmallStr::new(value), expires_ms);
      self.data.set_str_hinted_pinned(key, hash, store_val);
      if expires_ms != 0 {
         self.add_ttl();
      }
   }

   #[inline(always)]
   pub fn set_string_hinted_slot(
      &self,
      key: &str,
      hash: u64,
      _slot_hint: *const (),
      value: &str,
      expires_ms: u64,
   ) {
      let idx = self.data.shard_of(hash);
      let store_val = StoreValue::string_small_with_expiry(SmallStr::new(value), expires_ms);
      if self
         .data
         .insert_slot_fast_located(hash, idx, key, store_val)
      {
         self.data.touched(key);
         if expires_ms != 0 {
            self.add_ttl();
         }
         return;
      }
      self.set_string_hinted_pinned(key, Some(hash), value, expires_ms);
   }

   #[cold]
   #[inline(never)]
   fn set_string_fallback(&self, key: &str, hash: u64, value: &str, expires_ms: u64) {
      let store_val = StoreValue::string_small_with_expiry(SmallStr::new(value), expires_ms);
      self.data.set_str_hinted_pinned(key, Some(hash), store_val);
      if expires_ms != 0 {
         self.add_ttl();
      }
   }

   #[inline(always)]
   pub fn set_string_with_hint(
      &self,
      key: &str,
      hash: u64,
      entry_hint: *const (),
      value: &str,
      expires_ms: u64,
   ) {
      unsafe {
         self.set_string_fast_entry(key, hash, entry_hint.cast(), value, expires_ms);
      }
   }

   #[inline]
   pub fn try_set_string(&self, key: &str, value: &str, expires_ms: u64) -> Result<(), Full> {
      let store_val = StoreValue::string_small_with_expiry(SmallStr::new(value), expires_ms);
      self.data.try_set_str(key, store_val)?;
      if expires_ms != 0 {
         self.add_ttl();
      }
      Ok(())
   }

   pub fn get(&self, key: &str) -> Option<String> {
      let (h, idx) = self.data.locate_key(key);
      let p = self.data.probe_slot_entry(h, idx);
      if !p.is_null() {
         let e = unsafe { &*p };
         let state = e.state.load(std::sync::atomic::Ordering::Acquire);
         let expected = ((h as u32 as u64) << 32) | customhash::STATE_OCCUPIED;
         if (state & customhash::STATE_MATCH_MASK) == expected && e.key.eq_str(key) {
            let val = unsafe { (*e.value.get()).assume_init_ref() };
            if !val.is_expired() {
               return val.value.as_string().map(|s| s.to_string());
            } else {
               self.data.remove(key);
               return None;
            }
         }
      }
      let result = self.data.with_entry(key, h, idx, |val| {
         if val.is_expired() {
            return Err(());
         }
         Ok(val.value.as_string().map(|s| s.to_string()))
      })?;
      match result {
         Err(()) => {
            self.data.remove(key);
            None
         }
         Ok(v) => v,
      }
   }

   #[inline]
   pub fn get_to_buf(&self, key: &str, out: &mut Vec<u8>) -> bool {
      self.get_to_buf_hinted(key, None, out)
   }

   #[inline(always)]
   pub fn get_to_buf_hinted(&self, key: &str, hash: Option<u64>, out: &mut Vec<u8>) -> bool {
      self.get_to_buf_hinted_bytes(key.as_bytes(), hash, out)
   }

   #[inline(always)]
   pub fn get_to_buf_hinted_bytes(&self, key: &[u8], hash: Option<u64>, out: &mut Vec<u8>) -> bool {
      let (h, idx) = match hash {
         Some(h) => (h, self.data.shard_of(h)),
         None => {
            if let Ok(s) = std::str::from_utf8(key) {
               self.data.locate_key(s)
            } else {
               return false;
            }
         }
      };
      let p = self.data.probe_slot_entry(h, idx);
      if !p.is_null() {
         unsafe {
            if self.get_to_buf_fast_entry_bytes(key, h, p, out) {
               return true;
            }
         }
      }
      if let Ok(s) = std::str::from_utf8(key) {
         self.get_to_buf_fallback(s, h, out)
      } else {
         false
      }
   }

   #[cold]
   #[inline(never)]
   fn get_to_buf_fallback(&self, key: &str, hash: u64, out: &mut Vec<u8>) -> bool {
      let (h, idx) = (hash, self.data.shard_of(hash));
      let mut expired = false;
      let matched = self.data.with_entry_pinned(key, h, idx, |val| {
         if val.is_expired() {
            expired = true;
            return false;
         }
         if let crate::storage::value::FyroDB::String(s) = &val.value {
            s.write_resp(out);
            return true;
         }
         false
      });
      if expired {
         self.data.remove(key);
         return false;
      }
      matched.unwrap_or(false)
   }

   /// # Safety
   ///
   /// `p` must be null or point to a valid `Entry<StoreValue>`.
   #[inline(always)]
   pub unsafe fn get_to_buf_fast_entry_bytes(
      &self,
      key: &[u8],
      hash: u64,
      p: *const customhash::Entry<StoreValue>,
      out: &mut Vec<u8>,
   ) -> bool {
      if !p.is_null() {
         let e = unsafe { &*p };
         let state = e.state.load(std::sync::atomic::Ordering::Relaxed);
         let expected = ((hash as u32 as u64) << 32) | customhash::STATE_OCCUPIED;
         if (state & (0xFFFF_FFFF_0000_0000 | customhash::STATE_OCCUPIED)) == expected
            && e.key.eq_bytes(key)
         {
            if state & customhash::STATE_LOCK != 0 {
               let mut cur = state;
               while cur & customhash::STATE_LOCK != 0 {
                  std::hint::spin_loop();
                  cur = e.state.load(std::sync::atomic::Ordering::Acquire);
               }
               if cur & customhash::STATE_OCCUPIED == 0 {
                  return false;
               }
            }
            std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
            let val = unsafe { (*e.value.get()).assume_init_ref() };
            if !val.is_expired() {
               if let crate::storage::value::FyroDB::String(s) = &val.value {
                  s.write_resp(out);
                  return true;
               }
            } else {
               if let Ok(key_str) = std::str::from_utf8(key) {
                  self.data.remove(key_str);
               }
               return false;
            }
         }
      }
      if let Ok(key_str) = std::str::from_utf8(key) {
         self.get_to_buf_fallback(key_str, hash, out)
      } else {
         false
      }
   }

   /// # Safety
   ///
   /// `p` must be null or point to a valid `Entry<StoreValue>`.
   #[inline(always)]
   pub unsafe fn get_to_buf_fast_entry(
      &self,
      key: &str,
      hash: u64,
      p: *const customhash::Entry<StoreValue>,
      out: &mut Vec<u8>,
   ) -> bool {
      unsafe { self.get_to_buf_fast_entry_bytes(key.as_bytes(), hash, p, out) }
   }

   /// # Safety
   ///
   /// `p` must be null or point to a valid `Entry<StoreValue>`.
   #[inline(always)]
   pub unsafe fn set_string_fast_entry(
      &self,
      key: &str,
      hash: u64,
      p: *const customhash::Entry<StoreValue>,
      value: &str,
      expires_ms: u64,
   ) {
      if !p.is_null() {
         let e = unsafe { &*p };
         if e.key.eq_str(key) {
            let mut current = e.state.load(std::sync::atomic::Ordering::Relaxed);
            for _ in 0..32 {
               while current & customhash::STATE_LOCK != 0 {
                  std::hint::spin_loop();
                  current = e.state.load(std::sync::atomic::Ordering::Relaxed);
               }
               if (current & customhash::STATE_OCCUPIED) == 0 {
                  break;
               }
               let locked = customhash::bump_seq(current) | customhash::STATE_LOCK;
               match e.state.compare_exchange_weak(
                  current,
                  locked,
                  std::sync::atomic::Ordering::Acquire,
                  std::sync::atomic::Ordering::Relaxed,
               ) {
                  Ok(_) => {
                     self.data.snap_before_write(e);
                     let val = unsafe { (*e.value.get()).assume_init_mut() };
                     val.set_expires_ms(expires_ms);
                     if let crate::storage::value::FyroDB::String(ref mut s) = val.value {
                        s.overwrite_str(value);
                     } else {
                        val.value = crate::storage::value::FyroDB::String(SmallStr::new(value));
                     }
                     customhash::unlock_write(&e.state, locked, true);
                     self.data.touched(key);
                     if expires_ms != 0 {
                        self.add_ttl();
                     }
                     return;
                  }
                  Err(observed) => {
                     current = observed;
                     std::hint::spin_loop();
                  }
               }
            }
         }
      }
      self.set_string_fallback(key, hash, value, expires_ms);
   }

   #[inline(always)]
   pub fn set_string_auto(
      &self,
      key: &str,
      hash_hint: Option<u64>,
      entry_hint: *const (),
      value: &str,
      expires_ms: u64,
   ) {
      if !entry_hint.is_null() {
         let hash = hash_hint.unwrap_or_else(|| self.data.locate_key(key).0);
         unsafe {
            self.set_string_fast_entry(key, hash, entry_hint.cast(), value, expires_ms);
         }
         return;
      }
      let hash = match hash_hint {
         Some(h) => h,
         None => self.data.locate_key(key).0,
      };
      let idx = self.data.shard_of(hash);
      let store_val = StoreValue::string_small_with_expiry(SmallStr::new(value), expires_ms);
      if self
         .data
         .insert_slot_fast_located(hash, idx, key, store_val)
      {
         self.data.touched(key);
         if expires_ms != 0 {
            self.add_ttl();
         }
         return;
      }
      match self.data.probe_or_insert_fast(hash, key, || {
         StoreValue::string_small_with_expiry(SmallStr::new(value), expires_ms)
      }) {
         customhash::FastSet::Existing(entry) => unsafe {
            self.set_string_fast_entry(key, hash, entry, value, expires_ms);
         },
         customhash::FastSet::Inserted => {
            self.data.touched(key);
            if expires_ms != 0 {
               self.add_ttl();
            }
         }
         customhash::FastSet::Retry => self.set_string_fallback(key, hash, value, expires_ms),
      }
   }

   #[inline(always)]
   pub fn set_string_fast_bytes(&self, key: &str, hash: u64, val_bytes: &[u8]) {
      let idx = self.data.shard_of(hash);
      let store_val = StoreValue::string_small(SmallStr::from_slice(val_bytes));
      if self
         .data
         .insert_slot_fast_located(hash, idx, key, store_val)
      {
         self.data.touched(key);
         return;
      }
      if let Ok(val_str) = std::str::from_utf8(val_bytes) {
         self.set_string_auto(key, Some(hash), std::ptr::null(), val_str, 0);
      }
   }

   #[inline(always)]
   pub fn get_to_buf_auto(
      &self,
      key: &str,
      hash_hint: Option<u64>,
      entry_hint: *const (),
      out: &mut Vec<u8>,
   ) -> bool {
      if !entry_hint.is_null() {
         let hash = hash_hint.unwrap_or_else(|| self.data.locate_key(key).0);
         unsafe {
            if self.get_to_buf_fast_entry(key, hash, entry_hint.cast(), out) {
               return true;
            }
         }
      }
      let (hash, idx) = match hash_hint {
         Some(h) => (h, self.data.shard_of(h)),
         None => self.data.locate_key(key),
      };
      if let Some(entry) = self.data.probe_entry_fast(key, hash, idx) {
         unsafe {
            if self.get_to_buf_fast_entry(key, hash, entry, out) {
               return true;
            }
         }
      }
      self.get_to_buf_fallback(key, hash, out)
   }

   #[inline(always)]
   pub fn get_to_buf_with_hint(
      &self,
      key: &str,
      hash: u64,
      entry_hint: *const (),
      out: &mut Vec<u8>,
   ) -> bool {
      unsafe { self.get_to_buf_fast_entry(key, hash, entry_hint.cast(), out) }
   }

   #[inline(always)]
   pub fn get_to_buf_with_hint_bytes(
      &self,
      key: &[u8],
      hash: u64,
      entry_hint: *const (),
      out: &mut Vec<u8>,
   ) -> bool {
      unsafe { self.get_to_buf_fast_entry_bytes(key, hash, entry_hint.cast(), out) }
   }

   #[inline(always)]
   pub fn get_to_buf_hinted_pinned(&self, key: &str, hash: Option<u64>, out: &mut Vec<u8>) -> bool {
      let (h, idx) = match hash {
         Some(h) => (h, self.data.shard_of(h)),
         None => self.data.locate_key(key),
      };
      let mut expired = false;
      let matched = self.data.with_entry_pinned(key, h, idx, |val| {
         if val.is_expired() {
            expired = true;
            return false;
         }
         if let crate::storage::value::FyroDB::String(s) = &val.value {
            s.write_resp(out);
            return true;
         }
         false
      });
      if expired {
         self.data.remove(key);
         return false;
      }
      matched.unwrap_or(false)
   }

   pub fn getdel(&self, key: &str) -> Option<SmallStr> {
      let entry = self.data.remove(key)?;
      if entry.is_expired() {
         return None;
      }
      match entry.value {
         crate::storage::value::FyroDB::String(s) => Some(s),
         _ => None,
      }
   }

   pub fn getset(&self, key: &str, new_value: &str) -> Option<SmallStr> {
      let result = self.data.update_with(key, |val| {
         let old = if val.is_expired() {
            None
         } else {
            val.value.as_string().cloned()
         };
         *val = StoreValue::string_small(SmallStr::new(new_value));
         old
      });
      match result {
         Some(old) => old,
         None => {
            self
               .data
               .insert_str(key, StoreValue::string_small(SmallStr::new(new_value)));
            None
         }
      }
   }

   pub fn getex_ms(&self, key: &str, expires_ms: u64) -> Option<SmallStr> {
      let (result, ttl_change) = self.data.update_with(key, |val| {
         if val.is_expired() {
            return (None, 0i8);
         }
         let Some(s) = val.value.as_string().cloned() else {
            return (None, 0);
         };
         let ttl_change = match (val.get_expires_ms().is_none(), expires_ms == 0) {
            (true, false) => 1,
            (false, true) => -1,
            _ => 0,
         };
         val.set_expires_ms(expires_ms);
         (Some(s), ttl_change)
      })?;
      match ttl_change {
         1 => self.add_ttl(),
         -1 => self.sub_ttl(),
         _ => {}
      }
      result
   }

   pub fn setnx(&self, key: &str, value: &str) -> bool {
      let result = self.data.update_with(key, |current| {
         if current.is_expired() {
            *current = StoreValue::string_small(SmallStr::new(value));
            true
         } else {
            false
         }
      });
      match result {
         Some(did_set) => did_set,
         None => {
            let sv = StoreValue::string_small(SmallStr::new(value));
            self.data.insert_str(key, sv);
            true
         }
      }
   }

   pub fn append(&self, key: &str, suffix: &str) -> Result<usize, &'static str> {
      let result = self.data.update_with(key, |val| {
         if val.is_expired() {
            *val = StoreValue::string_small(SmallStr::new(suffix));
            return Ok(suffix.len());
         }
         match val.value.as_string_mut() {
            Some(s) => {
               s.push_str(suffix);
               Ok(s.len())
            }
            None => Err("WRONGTYPE"),
         }
      });

      match result {
         Some(r) => r,
         None => {
            let len = suffix.len();
            let sv = StoreValue::string_small(SmallStr::new(suffix));
            self.data.insert_str(key, sv);
            Ok(len)
         }
      }
   }

   pub fn strlen(&self, key: &str) -> Result<usize, &'static str> {
      match self.data.get_ref(key) {
         None => Ok(0),
         Some(e) if e.is_expired() => Ok(0),
         Some(e) => match e.value.as_string() {
            Some(s) => Ok(s.len()),
            None => Err("WRONGTYPE"),
         },
      }
   }

   pub fn getrange_to_buf(&self, key: &str, start: i64, end: i64, out: &mut Vec<u8>) -> bool {
      let entry = match self.data.get_ref(key) {
         Some(e) if !e.is_expired() => e,
         _ => {
            crate::utils::resp::write_bulk(out, "");
            return true;
         }
      };
      let s = match entry.value.as_string() {
         Some(s) => s,
         None => {
            crate::utils::resp::write_bulk(out, "");
            return true;
         }
      };

      let len = s.len() as i64;
      if len == 0 {
         crate::utils::resp::write_bulk(out, "");
         return true;
      }

      let mut start = if start < 0 {
         (len + start).max(0)
      } else {
         start.min(len)
      } as usize;
      let mut end = if end < 0 {
         (len + end).max(0)
      } else {
         end.min(len - 1)
      } as usize;

      if start > end {
         crate::utils::resp::write_bulk(out, "");
         return true;
      }

      while start > 0 && !s.is_char_boundary(start) {
         start -= 1;
      }
      end += 1;
      while end < s.len() && !s.is_char_boundary(end) {
         end += 1;
      }
      crate::utils::resp::write_bulk(out, &s[start..end]);
      true
   }

   pub fn getrange(&self, key: &str, start: i64, end: i64) -> String {
      let entry = match self.data.get_ref(key) {
         Some(e) if !e.is_expired() => e,
         _ => return String::new(),
      };
      let s = match entry.value.as_string() {
         Some(s) => s,
         None => return String::new(),
      };

      let len = s.len() as i64;
      if len == 0 {
         return String::new();
      }

      let mut start = if start < 0 {
         (len + start).max(0)
      } else {
         start.min(len)
      } as usize;
      let mut end = if end < 0 {
         (len + end).max(0)
      } else {
         end.min(len - 1)
      } as usize;

      if start > end {
         return String::new();
      }

      while start > 0 && !s.is_char_boundary(start) {
         start -= 1;
      }
      end += 1;
      while end < s.len() && !s.is_char_boundary(end) {
         end += 1;
      }
      s[start..end].to_string()
   }

   pub fn setrange(&self, key: &str, offset: usize, value: &str) -> Result<usize, &'static str> {
      let Some(needed) = offset.checked_add(value.len()) else {
         return Err("offset is out of range");
      };
      if needed > MAX_STRING_BYTES {
         return Err("string exceeds maximum allowed size");
      }

      let result = self
         .data
         .update_with(key, |val| match val.value.as_string_mut() {
            Some(s) => {
               let mut bytes = std::mem::take(s).into_bytes();
               if bytes.len() < needed {
                  bytes.resize(needed, 0u8);
               }
               bytes[offset..offset + value.len()].copy_from_slice(value.as_bytes());
               if std::str::from_utf8(&bytes).is_err() {
                  *s = SmallStr::from_bytes(bytes);
                  return Err("result would not be valid UTF-8");
               }
               let len = bytes.len();
               *s = SmallStr::from_bytes(bytes);
               Ok(len)
            }
            None => Err("WRONGTYPE"),
         });

      match result {
         Some(r) => r,
         None => {
            let mut bytes = vec![0u8; needed];
            bytes[offset..].copy_from_slice(value.as_bytes());
            if std::str::from_utf8(&bytes).is_err() {
               return Err("result would not be valid UTF-8");
            }
            let len = bytes.len();
            let new_str = SmallStr::from_bytes(bytes);
            if self
               .data
               .insert_if_absent_str(key, StoreValue::string_small(new_str))
            {
               Ok(len)
            } else {
               self.setrange(key, offset, value)
            }
         }
      }
   }

   #[inline]
   fn int_op(&self, key: &str, delta: i64) -> Result<i64, &'static str> {
      self.int_op_hinted(key, None, delta)
   }

   fn int_op_hinted(&self, key: &str, hash: Option<u64>, delta: i64) -> Result<i64, &'static str> {
      loop {
         let result = self
            .data
            .update_with_hinted(key, hash, |val| match val.value.as_string() {
               Some(s) => {
                  let n = match s.parse::<i64>() {
                     Ok(n) => n,
                     Err(_) => return Err("value is not an integer or out of range"),
                  };
                  let Some(new) = n.checked_add(delta) else {
                     return Err("increment or decrement would overflow");
                  };
                  val.value = crate::storage::value::FyroDB::String(SmallStr::from_int(new));
                  Ok(new)
               }
               None => Err("WRONGTYPE"),
            });
         if let Some(r) = result {
            return r;
         }
         // Absent: lock-free create. Losing the insert race just means the
         // key now exists, so the loop retries the in-place update.
         if self.data.insert_if_absent_str_hinted(
            key,
            hash,
            StoreValue::string_small(SmallStr::from_int(delta)),
         ) {
            return Ok(delta);
         }
      }
   }

   #[inline]
   pub fn incr(&self, key: &str) -> Result<i64, &'static str> {
      self.int_op_hinted(key, None, 1)
   }
   #[inline]
   pub fn incr_hinted(&self, key: &str, hash: Option<u64>) -> Result<i64, &'static str> {
      self.int_op_hinted(key, hash, 1)
   }
   pub fn decr(&self, key: &str) -> Result<i64, &'static str> {
      self.int_op(key, -1)
   }
   pub fn incrby(&self, key: &str, by: i64) -> Result<i64, &'static str> {
      self.int_op(key, by)
   }
   pub fn decrby(&self, key: &str, by: i64) -> Result<i64, &'static str> {
      let delta = by
         .checked_neg()
         .ok_or("increment or decrement would overflow")?;
      self.int_op(key, delta)
   }

   pub fn incrbyfloat(&self, key: &str, by: f64) -> Result<f64, &'static str> {
      let result = self
         .data
         .update_with(key, |val| match val.value.as_string() {
            Some(s) => {
               let n = match s.parse::<f64>() {
                  Ok(n) => n,
                  Err(_) => return Err("value is not a valid float"),
               };
               let new = n + by;
               val.value = crate::storage::value::FyroDB::String(SmallStr::from_float(new));
               Ok(new)
            }
            None => Err("WRONGTYPE"),
         });

      match result {
         Some(r) => r,
         None => {
            self
               .data
               .insert_str(key, StoreValue::string_small(SmallStr::from_float(by)));
            Ok(by)
         }
      }
   }
}

impl Store {
   pub fn lcs(&self, key1: &str, key2: &str) -> Result<String, &'static str> {
      let ref1 = self.data.get_ref(key1);
      let ref2 = self.data.get_ref(key2);

      let s1 = match ref1.as_deref() {
         None => "",
         Some(e) if e.is_expired() => "",
         Some(e) => match e.value.as_string() {
            Some(s) => s.as_str(),
            None => return Err("WRONGTYPE"),
         },
      };
      let s2 = match ref2.as_deref() {
         None => "",
         Some(e) if e.is_expired() => "",
         Some(e) => match e.value.as_string() {
            Some(s) => s.as_str(),
            None => return Err("WRONGTYPE"),
         },
      };

      let mut result = Vec::new();
      hirschberg(s1.as_bytes(), s2.as_bytes(), &mut result);
      Ok(String::from_utf8(result).unwrap_or_default())
   }

   pub fn lcs_len(&self, key1: &str, key2: &str) -> Result<usize, &'static str> {
      let ref1 = self.data.get_ref(key1);
      let ref2 = self.data.get_ref(key2);

      let s1 = match ref1.as_deref() {
         None => "",
         Some(e) if e.is_expired() => "",
         Some(e) => match e.value.as_string() {
            Some(s) => s.as_str(),
            None => return Err("WRONGTYPE"),
         },
      };
      let s2 = match ref2.as_deref() {
         None => "",
         Some(e) if e.is_expired() => "",
         Some(e) => match e.value.as_string() {
            Some(s) => s.as_str(),
            None => return Err("WRONGTYPE"),
         },
      };

      let b1 = s1.as_bytes();
      let b2 = s2.as_bytes();
      let m = b1.len();
      let n = b2.len();

      if m == 0 || n == 0 {
         return Ok(0);
      }

      let mut prev = vec![0u16; n + 1];
      let mut curr = vec![0u16; n + 1];
      for i in 1..=m {
         for j in 1..=n {
            curr[j] = if b1[i - 1] == b2[j - 1] {
               prev[j - 1] + 1
            } else {
               prev[j].max(curr[j - 1])
            };
         }
         std::mem::swap(&mut prev, &mut curr);
         curr.fill(0);
      }
      Ok(prev[n] as usize)
   }
}
