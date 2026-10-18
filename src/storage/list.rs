use crate::storage::store::Store;
use crate::storage::value::{FyroDB, ListInner, StoreValue};

impl Store {
   fn list_push(&self, key: &str, values: &[&str], front: bool) -> Result<usize, &'static str> {
      let push = |l: &mut ListInner| {
         for v in values {
            if front {
               l.push_front(v);
            } else {
               l.push_back(v);
            }
         }
         l.len()
      };
      loop {
         let result = self.data.update_with(key, |val| {
            if val.is_expired() {
               let mut l = ListInner::new();
               let n = push(&mut l);
               val.value = FyroDB::List(Box::new(l));
               val.expires_ms = 0;
               return Ok(n);
            }
            match val.value.as_list_mut() {
               Some(l) => Ok(push(l)),
               None => Err("WRONGTYPE"),
            }
         });
         if let Some(r) = result {
            return r;
         }
         let mut l = ListInner::new();
         let n = push(&mut l);
         let v = StoreValue {
            value: FyroDB::List(Box::new(l)),
            expires_ms: 0,
         };
         if self.data.insert_if_absent_str(key, v) {
            return Ok(n);
         }
      }
   }

   pub fn lpush(&self, key: &str, values: &[&str]) -> Result<usize, &'static str> {
      let r = self.list_push(key, values, true);
      if r.is_ok() {
         self.blocking.signal(key);
      }
      r
   }

   pub fn rpush(&self, key: &str, values: &[&str]) -> Result<usize, &'static str> {
      let r = self.list_push(key, values, false);
      if r.is_ok() {
         self.blocking.signal(key);
      }
      r
   }

   fn list_write<R: Default>(
      &self,
      key: &str,
      f: impl FnOnce(&mut ListInner) -> R,
   ) -> Result<R, &'static str> {
      let mut f = Some(f);
      let result = self.data.update_with(key, |val| {
         if val.is_expired() {
            return Ok(R::default());
         }
         match val.value.as_list_mut() {
            Some(l) => Ok((f.take().expect("once"))(l)),
            None => Err("WRONGTYPE"),
         }
      });
      result.unwrap_or(Ok(R::default()))
   }

   fn list_read<R: Default>(
      &self,
      key: &str,
      f: impl FnOnce(&ListInner) -> R,
   ) -> Result<R, &'static str> {
      match self.data.get_ref(key) {
         None => Ok(R::default()),
         Some(e) if e.is_expired() => Ok(R::default()),
         Some(e) => match e.value.as_list() {
            Some(l) => Ok(f(l)),
            None => Err("WRONGTYPE"),
         },
      }
   }

   pub fn lpop(&self, key: &str, count: usize) -> Result<Vec<String>, &'static str> {
      self.list_write(key, |l| {
         let n = count.min(l.len());
         let mut v = Vec::with_capacity(n);
         v.extend((0..n).map_while(|_| l.pop_front()));
         v
      })
   }

   pub fn rpop(&self, key: &str, count: usize) -> Result<Vec<String>, &'static str> {
      self.list_write(key, |l| {
         let n = count.min(l.len());
         let mut v = Vec::with_capacity(n);
         v.extend((0..n).map_while(|_| l.pop_back()));
         v
      })
   }

   pub fn pop_many_to_buf(
      &self,
      key: &str,
      count: usize,
      from_back: bool,
      out: &mut Vec<u8>,
   ) -> Result<usize, &'static str> {
      self.list_write(key, |l| {
         let mut n = 0;
         while n < count {
            let write = |s: &str| crate::utils::resp::write_bulk(out, s);
            let hit = if from_back {
               l.pop_back_with(write)
            } else {
               l.pop_front_with(write)
            };
            if hit.is_none() {
               break;
            }
            n += 1;
         }
         n
      })
   }

   pub fn pop_one_to_buf(
      &self,
      key: &str,
      from_back: bool,
      out: &mut Vec<u8>,
   ) -> Result<bool, &'static str> {
      self.list_write(key, |l| {
         let write = |s: &str| crate::utils::resp::write_bulk(out, s);
         if from_back {
            l.pop_back_with(write).is_some()
         } else {
            l.pop_front_with(write).is_some()
         }
      })
   }

   pub fn pop_one_to_scratch(
      &self,
      key: &str,
      from_back: bool,
      scratch: &mut String,
   ) -> Result<bool, &'static str> {
      self.list_write(key, |l| {
         let copy = |s: &str| {
            scratch.clear();
            scratch.push_str(s);
         };
         if from_back {
            l.pop_back_with(copy).is_some()
         } else {
            l.pop_front_with(copy).is_some()
         }
      })
   }

   pub fn llen(&self, key: &str) -> Result<usize, &'static str> {
      self.list_read(key, |l| l.len())
   }

   pub fn lindex(&self, key: &str, index: i64) -> Result<Option<String>, &'static str> {
      self.list_read(key, |l| {
         normalize_index(index, l.len()).and_then(|i| l.get(i).map(str::to_string))
      })
   }

   pub fn lset(&self, key: &str, index: i64, value: &str) -> Result<bool, &'static str> {
      let result = self.data.update_with(key, |val| {
         if val.is_expired() {
            return Err("no such key");
         }
         match val.value.as_list_mut() {
            Some(l) => match normalize_index(index, l.len()) {
               Some(i) => Ok(l.set(i, value)),
               None => Err("index out of range"),
            },
            None => Err("WRONGTYPE"),
         }
      });
      result.unwrap_or(Err("no such key"))
   }

   pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, &'static str> {
      let result = self.data.read_consistent(key, |val| {
         if val.is_expired() {
            return Ok(vec![]);
         }
         match val.value.as_list() {
            Some(l) => Ok(match range_bounds(start, stop, l.len()) {
               Some((s, n)) => l.range(s, n).map(str::to_string).collect(),
               None => vec![],
            }),
            None => Err("WRONGTYPE"),
         }
      });
      result.unwrap_or(Ok(vec![]))
   }

   pub fn lrange_to_buf(
      &self,
      key: &str,
      start: i64,
      stop: i64,
      out: &mut Vec<u8>,
   ) -> Result<usize, &'static str> {
      let start_len = out.len();
      let result = self.data.read_consistent(key, |val| {
         out.truncate(start_len);
         if val.is_expired() {
            crate::utils::resp::write_array_header(out, 0);
            return Ok(0);
         }
         match val.value.as_list() {
            Some(l) => {
               let Some((s, n)) = range_bounds(start, stop, l.len()) else {
                  crate::utils::resp::write_array_header(out, 0);
                  return Ok(0);
               };
               crate::utils::resp::write_array_header(out, n);
               for v in l.range(s, n) {
                  crate::utils::resp::write_bulk(out, v);
               }
               Ok(n)
            }
            None => Err("WRONGTYPE"),
         }
      });
      match result {
         Some(r) => r,
         None => {
            out.truncate(start_len);
            crate::utils::resp::write_array_header(out, 0);
            Ok(0)
         }
      }
   }

   pub fn ltrim(&self, key: &str, start: i64, stop: i64) -> Result<(), &'static str> {
      self.list_write(key, |l| match range_bounds(start, stop, l.len()) {
         Some((s, n)) => l.trim(s, n),
         None => l.clear(),
      })
   }

   pub fn lrem(&self, key: &str, count: i64, value: &str) -> Result<usize, &'static str> {
      self.list_write(key, |l| {
         let limit = if count == 0 {
            usize::MAX
         } else {
            count.unsigned_abs() as usize
         };
         let removed = l.remove_matching(value, limit, count < 0);
         l.shrink_to_fit();
         removed
      })
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
            Some(l) => match l.position(pivot) {
               Some(idx) => {
                  l.insert(if before { idx } else { idx + 1 }, value);
                  Ok(l.len() as i64)
               }
               None => Ok(-1),
            },
            None => Err("WRONGTYPE"),
         }
      });
      result.unwrap_or(Ok(0))
   }

   pub fn lpos(
      &self,
      key: &str,
      value: &str,
      rank: i64,
      count: usize,
      maxlen: usize,
   ) -> Result<Vec<usize>, &'static str> {
      self.list_read(key, |l| {
         let len = l.len();
         let max = if maxlen == 0 { len } else { maxlen.min(len) };
         let want = |n: usize| count > 0 && n >= count;
         let mut results = Vec::new();
         let mut found = 0usize;
         if rank >= 0 {
            let skip = if rank == 0 { 0 } else { (rank - 1) as usize };
            for (i, v) in l.iter().enumerate().take(max) {
               if v == value {
                  if found >= skip {
                     results.push(i);
                     if want(results.len()) {
                        break;
                     }
                  }
                  found += 1;
               }
            }
         } else {
            let skip = ((-rank) - 1) as usize;
            for (k, v) in l.iter().rev().enumerate().take(max) {
               if v == value {
                  if found >= skip {
                     results.push(len - 1 - k);
                     if want(results.len()) {
                        break;
                     }
                  }
                  found += 1;
               }
            }
         }
         results
      })
   }

   pub fn lmove(
      &self,
      src: &str,
      dst: &str,
      left_src: bool,
      left_dst: bool,
   ) -> Result<Option<String>, &'static str> {
      let r = self.lmove_impl(src, dst, left_src, left_dst);
      if r.is_ok() {
         self.blocking.signal(dst);
      }
      r
   }

   fn lmove_impl(
      &self,
      src: &str,
      dst: &str,
      left_src: bool,
      left_dst: bool,
   ) -> Result<Option<String>, &'static str> {
      let dst_bad = self
         .data
         .get_ref(dst)
         .is_some_and(|e| !e.is_expired() && e.value.as_list().is_none());
      if dst_bad {
         return if self.llen(src)? > 0 {
            Err("WRONGTYPE")
         } else {
            Ok(None)
         };
      }
      let popped: Option<String> = self.list_write(src, |l| {
         if left_src {
            l.pop_front()
         } else {
            l.pop_back()
         }
      })?;
      let Some(value) = popped else {
         return Ok(None);
      };
      let pushed = if left_dst {
         self.list_push(dst, &[&value], true)
      } else {
         self.list_push(dst, &[&value], false)
      };
      if let Err(e) = pushed {
         let _ = self.list_push(src, &[&value], left_src);
         return Err(e);
      }
      Ok(Some(value))
   }
}

#[inline]
fn range_bounds(start: i64, stop: i64, len: usize) -> Option<(usize, usize)> {
   let len = len as i64;
   if len == 0 {
      return None;
   }
   let s = if start < 0 {
      (len + start).max(0)
   } else {
      start.min(len)
   };
   let e = if stop < 0 {
      (len + stop).max(-1)
   } else {
      stop.min(len - 1)
   };
   if s > e || s >= len {
      return None;
   }
   Some((s as usize, (e - s + 1) as usize))
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
