use std::cell::RefCell;
use std::sync::Arc;

struct CachedBatch {
   channel: String,
   msg_count: usize,
   msg_ptr: *const u8,
   msg_len: usize,
   frame: Arc<[u8]>,
}

thread_local! {
   static BATCH_BUF: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
   static BATCH_CACHE: RefCell<Option<CachedBatch>> = const { RefCell::new(None) };
}

#[inline(always)]
fn resp_int_len(n: usize) -> usize {
   if n < 10 {
      1
   } else if n < 100 {
      2
   } else if n < 1000 {
      3
   } else if n < 10000 {
      4
   } else {
      let mut len = 5;
      let mut cur = 10000;
      while cur <= n / 10 {
         cur *= 10;
         len += 1;
      }
      len
   }
}

pub fn encode_message(channel: &str, message: &str) -> Arc<[u8]> {
   let ch_bytes = channel.as_bytes();
   let msg_bytes = message.as_bytes();
   let ch_len = ch_bytes.len();
   let msg_len = msg_bytes.len();
   let total =
      15 + 1 + resp_int_len(ch_len) + 2 + ch_len + 2 + 1 + resp_int_len(msg_len) + 2 + msg_len + 2;
   let mut out = Vec::with_capacity(total);
   out.extend_from_slice(b"*3\r\n$7\r\nmessage\r\n");
   bulk_into(&mut out, ch_bytes);
   bulk_into(&mut out, msg_bytes);
   out.into()
}

pub fn encode_batch(channel: &str, messages: &[&str]) -> Arc<[u8]> {
   if messages.is_empty() {
      return Arc::from([]);
   }

   let first = messages[0];
   let is_uniform_ptr = messages
      .iter()
      .all(|m| std::ptr::eq(m.as_ptr(), first.as_ptr()) && m.len() == first.len());

   if is_uniform_ptr {
      let cached = BATCH_CACHE.with_borrow(|cache| {
         if let Some(c) = cache.as_ref()
            && c.msg_count == messages.len()
            && c.msg_ptr == first.as_ptr()
            && c.msg_len == first.len()
            && c.channel == channel
         {
            return Some(Arc::clone(&c.frame));
         }
         None
      });
      if let Some(frame) = cached {
         return frame;
      }
   }

   let ch_bytes = channel.as_bytes();
   let ch_len = ch_bytes.len();

   let mut prefix_buf = [0u8; 64];
   let prefix_len = {
      let p = &mut prefix_buf[..];
      let h = b"*3\r\n$7\r\nmessage\r\n$";
      p[..h.len()].copy_from_slice(h);
      let mut off = h.len();
      if ch_len < 10 {
         p[off] = b'0' + ch_len as u8;
         off += 1;
      } else if ch_len < 100 {
         p[off] = b'0' + (ch_len / 10) as u8;
         p[off + 1] = b'0' + (ch_len % 10) as u8;
         off += 2;
      } else {
         let mut tmp = [0u8; 16];
         let mut n = ch_len;
         let mut idx = 0;
         while n > 0 {
            tmp[idx] = b'0' + (n % 10) as u8;
            n /= 10;
            idx += 1;
         }
         while idx > 0 {
            idx -= 1;
            p[off] = tmp[idx];
            off += 1;
         }
      }
      p[off..off + 2].copy_from_slice(b"\r\n");
      off += 2;
      p[off..off + ch_len].copy_from_slice(ch_bytes);
      off += ch_len;
      p[off..off + 2].copy_from_slice(b"\r\n");
      off += 2;
      off
   };
   let prefix = &prefix_buf[..prefix_len];

   let first_len = first.len();
   let uniform = is_uniform_ptr || messages.iter().all(|m| m.len() == first_len);

   if uniform {
      let mut hdr_buf = [0u8; 96];
      hdr_buf[..prefix_len].copy_from_slice(prefix);
      let mut h_off = prefix_len;
      hdr_buf[h_off] = b'$';
      h_off += 1;
      if first_len < 10 {
         hdr_buf[h_off] = b'0' + first_len as u8;
         h_off += 1;
      } else if first_len < 100 {
         hdr_buf[h_off] = b'0' + (first_len / 10) as u8;
         hdr_buf[h_off + 1] = b'0' + (first_len % 10) as u8;
         h_off += 2;
      } else {
         let mut tmp = [0u8; 16];
         let mut n = first_len;
         let mut idx = 0;
         while n > 0 {
            tmp[idx] = b'0' + (n % 10) as u8;
            n /= 10;
            idx += 1;
         }
         while idx > 0 {
            idx -= 1;
            hdr_buf[h_off] = tmp[idx];
            h_off += 1;
         }
      }
      hdr_buf[h_off..h_off + 2].copy_from_slice(b"\r\n");
      h_off += 2;
      let combined_hdr = &hdr_buf[..h_off];

      let block_len = combined_hdr.len() + first_len + 2;
      let total = block_len * messages.len();
      let mut arc = Arc::new_uninit_slice(total);
      let mut dst = Arc::get_mut(&mut arc).unwrap().as_mut_ptr() as *mut u8;

      let mut block_buf = [0u8; 128];
      if block_len <= 128 && is_uniform_ptr {
         unsafe {
            std::ptr::copy_nonoverlapping(
               combined_hdr.as_ptr(),
               block_buf.as_mut_ptr(),
               combined_hdr.len(),
            );
            std::ptr::copy_nonoverlapping(
               first.as_ptr(),
               block_buf.as_mut_ptr().add(combined_hdr.len()),
               first_len,
            );
            std::ptr::copy_nonoverlapping(
               b"\r\n".as_ptr(),
               block_buf.as_mut_ptr().add(combined_hdr.len() + first_len),
               2,
            );

            let src = block_buf.as_ptr();
            for _ in 0..messages.len() {
               std::ptr::copy_nonoverlapping(src, dst, block_len);
               dst = dst.add(block_len);
            }
         }
      } else {
         for &m in messages {
            unsafe {
               std::ptr::copy_nonoverlapping(combined_hdr.as_ptr(), dst, combined_hdr.len());
               dst = dst.add(combined_hdr.len());
               std::ptr::copy_nonoverlapping(m.as_ptr(), dst, first_len);
               dst = dst.add(first_len);
               std::ptr::copy_nonoverlapping(b"\r\n".as_ptr(), dst, 2);
               dst = dst.add(2);
            }
         }
      }
      let frame = unsafe { arc.assume_init() };
      if is_uniform_ptr {
         BATCH_CACHE.with_borrow_mut(|c| {
            *c = Some(CachedBatch {
               channel: channel.to_string(),
               msg_count: messages.len(),
               msg_ptr: first.as_ptr(),
               msg_len: first.len(),
               frame: Arc::clone(&frame),
            });
         });
      }
      frame
   } else {
      BATCH_BUF.with_borrow_mut(|buf| {
         buf.clear();
         let per_msg = prefix_len + 16 + first_len;
         buf.reserve(per_msg * messages.len());

         for &m in messages {
            buf.extend_from_slice(prefix);
            let m_bytes = m.as_bytes();
            let m_len = m_bytes.len();
            buf.push(b'$');
            if m_len < 10 {
               buf.push(b'0' + m_len as u8);
            } else if m_len < 100 {
               buf.push(b'0' + (m_len / 10) as u8);
               buf.push(b'0' + (m_len % 10) as u8);
            } else {
               push_usize(buf, m_len);
            }
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(m_bytes);
            buf.extend_from_slice(b"\r\n");
         }
         Arc::from(&buf[..])
      })
   }
}

pub fn encode_pmessage(pattern: &str, channel: &str, message: &str) -> Arc<[u8]> {
   let pat_len = pattern.len();
   let ch_len = channel.len();
   let msg_len = message.len();
   let total = 17
      + 1
      + resp_int_len(pat_len)
      + 2
      + pat_len
      + 2
      + 1
      + resp_int_len(ch_len)
      + 2
      + ch_len
      + 2
      + 1
      + resp_int_len(msg_len)
      + 2
      + msg_len
      + 2;
   let mut out = Vec::with_capacity(total);
   out.extend_from_slice(b"*4\r\n$8\r\npmessage\r\n");
   bulk_into(&mut out, pattern.as_bytes());
   bulk_into(&mut out, channel.as_bytes());
   bulk_into(&mut out, message.as_bytes());
   out.into()
}

pub fn encode_sub_reply(kind: &str, channel: &str, count: usize) -> Vec<u8> {
   let mut out = Vec::with_capacity(32 + kind.len() + channel.len());
   out.extend_from_slice(b"*3\r\n");
   bulk_into(&mut out, kind.as_bytes());
   bulk_into(&mut out, channel.as_bytes());
   out.push(b':');
   push_usize(&mut out, count);
   out.extend_from_slice(b"\r\n");
   out
}

#[inline(always)]
pub(crate) fn bulk_into(out: &mut Vec<u8>, b: &[u8]) {
   out.push(b'$');
   let len = b.len();
   if len < 10 {
      out.push(b'0' + len as u8);
   } else if len < 100 {
      out.push(b'0' + (len / 10) as u8);
      out.push(b'0' + (len % 10) as u8);
   } else {
      push_usize(out, len);
   }
   out.extend_from_slice(b"\r\n");
   out.extend_from_slice(b);
   out.extend_from_slice(b"\r\n");
}

#[inline]
pub(crate) fn push_usize(out: &mut Vec<u8>, mut n: usize) {
   if n == 0 {
      out.push(b'0');
      return;
   }
   let start = out.len();
   while n > 0 {
      out.push(b'0' + (n % 10) as u8);
      n /= 10;
   }
   out[start..].reverse();
}
