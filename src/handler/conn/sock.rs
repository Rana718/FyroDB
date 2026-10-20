use super::*;

impl<'a> Conn<'a> {
   pub fn do_read(&mut self) -> bool {
      loop {
         let buf = self.parser.read_buf();
         let cap = buf.len();
         match self.stream.read(buf) {
            Ok(0) => return false,
            Ok(n) => {
               self.parser.did_fill(n);
               if n < cap {
                  break;
               }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(_) => return false,
         }
      }

      #[cfg(feature = "bench-tools")]
      if is_raw_arrival_only() {
         let unparsed = self.parser.filled - self.parser.pos;
         record_raw_arrival(unparsed);
         self.parser.pos = self.parser.filled;
         return true;
      }

      if !self.process_buffered() {
         return false;
      }
      #[cfg(not(feature = "io-uring"))]
      {
         self.do_write()
      }
      #[cfg(feature = "io-uring")]
      {
         true
      }
   }

   /// Parse and execute commands in read buffer, pausing if parked and
   /// preserving unparsed bytes. Returns false on protocol error.
   pub fn process_buffered(&mut self) -> bool {
      let _guard = customhash::pin();
      let mut prefetched = 0usize;
      self.hint_n = 0;
      loop {
         if self.park.is_some() {
            return true;
         }
         if self.parser.pos >= prefetched && !self.parser.in_partial() {
            prefetched = self.prefetch_ahead();
         }
         match self.parser.parse_one() {
            ParseResult::Complete => {
               let raw_ptr = self.parser.parts_raw.as_ptr();
               let raw_len = self.parser.parts_raw.len();
               let raw = unsafe { std::slice::from_raw_parts(raw_ptr, raw_len) };
               if self.authenticated && !self.store.cluster.enabled && !self.store.at_key_capacity()
               {
                  let cmd_len = if raw.is_empty() { 0 } else { raw[0].1 };
                  let cmd = if raw.is_empty() {
                     &[][..]
                  } else {
                     unsafe { part_bytes(raw[0]) }
                  };

                  if cmd_len == 3 {
                     if cmd.eq_ignore_ascii_case(b"GET") && raw.len() == 2 {
                        dispatch_get_run(self, raw[1]);
                        prefetched = self.parser.pos;
                        continue;
                     }
                     if cmd.eq_ignore_ascii_case(b"SET") && raw.len() == 3 {
                        if is_set_expire_pair(self, raw) {
                           match dispatch_set_expire_pair(self, raw) {
                              PairOutcome::Done => continue,
                              PairOutcome::FallThrough => {}
                              PairOutcome::ConnError => return false,
                           }
                        }
                        dispatch_set_run(self, raw[1], raw[2]);
                        prefetched = self.parser.pos;
                        continue;
                     }
                  }

                  if cmd_len == 8
                     && cmd.eq_ignore_ascii_case(b"JSON.SET")
                     && dispatch_json_set_run(self, raw)
                  {
                     prefetched = self.parser.pos;
                     continue;
                  }

                  if (cmd_len == 4 || cmd_len == 5) && is_coalescible_queue(raw) {
                     match dispatch_queue_run(self, raw) {
                        QueueRunOutcome::Done => continue,
                        QueueRunOutcome::ConnError => return false,
                     }
                  }
                  if cmd_len == 7 && cmd.eq_ignore_ascii_case(b"PUBLISH") && is_publish_run(raw) {
                     match dispatch_publish_run(self, raw) {
                        QueueRunOutcome::Done => continue,
                        QueueRunOutcome::ConnError => return false,
                     }
                  }
                  if (cmd_len == 3 || cmd_len == 4 || cmd_len == 5 || cmd_len == 6 || cmd_len == 8)
                     && is_write_run(raw)
                     && self.next_is_same_write(raw)
                  {
                     match dispatch_write_run(self, raw) {
                        QueueRunOutcome::Done => continue,
                        QueueRunOutcome::ConnError => return false,
                     }
                  }
               }
               dispatch_raw(self, raw);
            }
            ParseResult::Incomplete => break,
            ParseResult::Error => return false,
         }
      }

      self.hint_n = 0;
      self.parser.release_read_buffer();

      true
   }

   pub fn do_write(&mut self) -> bool {
      if let ConnMode::Subscribed { ref slot, .. } = self.mode
         && self.parser.wbuf.len() < SUB_WRITE_BATCH_BYTES
      {
         slot.drain_into_limit(&mut self.parser.wbuf, SUB_WRITE_BATCH_BYTES);
      }

      if self.write_offset >= self.parser.wbuf.len() {
         self.parser.wbuf.clear();
         self.write_offset = 0;
         return true;
      }

      loop {
         match self.stream.write(&self.parser.wbuf[self.write_offset..]) {
            Ok(0) => return false,
            Ok(n) => {
               self.write_offset += n;
               if self.write_offset >= self.parser.wbuf.len() {
                  self.parser.wbuf.clear();
                  self.write_offset = 0;
                  if let ConnMode::Subscribed { ref slot, .. } = self.mode
                     && slot.has_pending()
                  {
                     slot.drain_into_limit(&mut self.parser.wbuf, SUB_WRITE_BATCH_BYTES);
                     continue;
                  }
                  if self.parser.wbuf.capacity() > RETAINED_WRITE_BUFFER {
                     self.parser.wbuf.shrink_to(RETAINED_WRITE_BUFFER);
                  }
                  return true;
               }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
               // Only compact if write_offset has advanced past a threshold to avoid memmove churn.
               if self.write_offset > 512 * 1024 && self.write_offset > self.parser.wbuf.len() / 2 {
                  self.parser.wbuf.drain(..self.write_offset);
                  self.write_offset = 0;
               }
               return true;
            }
            Err(_) => return false,
         }
      }
   }

   #[cfg(feature = "io-uring")]
   pub fn prepare_send(&mut self) -> Option<(std::os::fd::RawFd, *const u8, usize)> {
      use std::os::fd::AsRawFd;
      if let ConnMode::Subscribed { ref slot, .. } = self.mode
         && self.parser.wbuf.len() < SUB_WRITE_BATCH_BYTES
      {
         slot.drain_into_limit(&mut self.parser.wbuf, SUB_WRITE_BATCH_BYTES);
      }
      let pending = &self.parser.wbuf[self.write_offset..];
      if pending.is_empty() {
         return None;
      }
      Some((self.stream.as_raw_fd(), pending.as_ptr(), pending.len()))
   }

   #[cfg(feature = "io-uring")]
   pub fn finish_send(&mut self, res: i32) -> bool {
      if res == -libc::EAGAIN || res == -libc::EINTR {
         return true;
      }
      if res <= 0 {
         return false;
      }
      self.write_offset += res as usize;
      if self.write_offset >= self.parser.wbuf.len() {
         self.parser.wbuf.clear();
         self.write_offset = 0;
         if self.parser.wbuf.capacity() > RETAINED_WRITE_BUFFER {
            self.parser.wbuf.shrink_to(RETAINED_WRITE_BUFFER);
         }
      } else if self.write_offset > 512 * 1024 && self.write_offset > self.parser.wbuf.len() / 2 {
         self.parser.wbuf.drain(..self.write_offset);
         self.write_offset = 0;
      }
      true
   }

   pub fn has_pending_write(&self) -> bool {
      self.write_offset < self.parser.wbuf.len()
         || matches!(&self.mode, ConnMode::Subscribed { slot, .. } if slot.has_pending())
   }
}
