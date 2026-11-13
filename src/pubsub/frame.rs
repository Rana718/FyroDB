use std::cell::RefCell;
use std::mem::MaybeUninit;
use std::ops::Range;
use std::sync::Arc;

const MESSAGE: &[u8] = b"*3\r\n$7\r\nmessage\r\n";
const PMESSAGE: &[u8] = b"*4\r\n$8\r\npmessage\r\n";
const MAX_CACHED_FRAME_BYTES: usize = 8 * 1024 * 1024;

struct CachedBatch {
   fields: [Range<usize>; 2],
   field_count: usize,
   message: Range<usize>,
   count: usize,
   frame: Arc<[u8]>,
}

thread_local! {
   static BATCH_CACHE: RefCell<Option<CachedBatch>> = const { RefCell::new(None) };
}

#[inline]
fn int_len(mut value: usize) -> usize {
   let mut digits = 1;
   while value >= 10 {
      value /= 10;
      digits += 1;
   }
   digits
}

#[inline]
fn bulk_len(bytes: usize) -> usize {
   bytes
      .checked_add(int_len(bytes) + 5)
      .expect("pub/sub frame length overflow")
}

#[inline]
fn header(value: usize, out: &mut [u8; 24]) -> usize {
   out[0] = b'$';
   if value < 10 {
      out[1] = b'0' + value as u8;
      out[2..4].copy_from_slice(b"\r\n");
      return 4;
   }
   let mut digits = [0; 20];
   let number = crate::utils::resp::format_usize_digits(value, &mut digits);
   out[1..1 + number.len()].copy_from_slice(number);
   out[1 + number.len()..3 + number.len()].copy_from_slice(b"\r\n");
   number.len() + 3
}

#[inline]
fn append(out: &mut [MaybeUninit<u8>], at: &mut usize, bytes: &[u8]) {
   let end = at
      .checked_add(bytes.len())
      .expect("pub/sub frame length overflow");
   out[*at..end].write_copy_of_slice(bytes);
   *at = end;
}

#[inline]
fn bulk(out: &mut [MaybeUninit<u8>], at: &mut usize, bytes: &[u8]) {
   let mut h = [0; 24];
   let n = header(bytes.len(), &mut h);
   append(out, at, &h[..n]);
   append(out, at, bytes);
   append(out, at, b"\r\n");
}

fn encode_fields(prefix: &[u8], fields: &[&[u8]]) -> Arc<[u8]> {
   let total = fields.iter().fold(prefix.len(), |n, field| {
      n.checked_add(bulk_len(field.len()))
         .expect("pub/sub frame length overflow")
   });
   let mut arc = Arc::new_uninit_slice(total);
   let out = Arc::get_mut(&mut arc).unwrap();
   let mut at = 0;
   append(out, &mut at, prefix);
   for field in fields {
      bulk(out, &mut at, field);
   }
   assert_eq!(at, total);
   // Every byte was initialized through the checked destination slices.
   unsafe { arc.assume_init() }
}

pub fn encode_message(channel: &str, message: &str) -> Arc<[u8]> {
   encode_message_bytes(channel, message.as_bytes())
}

pub(crate) fn encode_message_bytes(channel: &str, message: &[u8]) -> Arc<[u8]> {
   encode_fields(MESSAGE, &[channel.as_bytes(), message])
}

pub fn encode_pmessage(pattern: &str, channel: &str, message: &str) -> Arc<[u8]> {
   encode_pmessage_bytes(pattern, channel, message.as_bytes())
}

pub(crate) fn encode_pmessage_bytes(pattern: &str, channel: &str, message: &[u8]) -> Arc<[u8]> {
   encode_fields(PMESSAGE, &[pattern.as_bytes(), channel.as_bytes(), message])
}

pub fn encode_batch(channel: &str, messages: &[&str]) -> Arc<[u8]> {
   encode_batch_bytes(channel, messages)
}

pub(crate) fn encode_batch_bytes<M: AsRef<[u8]>>(channel: &str, messages: &[M]) -> Arc<[u8]> {
   encode_batch_fields(MESSAGE, [channel.as_bytes()], messages)
}

pub(crate) fn encode_pmessage_batch<M: AsRef<[u8]>>(
   pattern: &str,
   channel: &str,
   messages: &[M],
) -> Arc<[u8]> {
   encode_batch_fields(PMESSAGE, [pattern.as_bytes(), channel.as_bytes()], messages)
}

fn encode_batch_fields<const FIELDS: usize, M: AsRef<[u8]>>(
   prefix: &[u8],
   fields: [&[u8]; FIELDS],
   messages: &[M],
) -> Arc<[u8]> {
   if messages.is_empty() {
      return Arc::from([]);
   }
   let first = messages[0].as_ref();
   let identical = messages.iter().all(|m| m.as_ref() == first);
   if identical
      && let Some(frame) = BATCH_CACHE.with_borrow(|cache| {
         cache.as_ref().and_then(|c| {
            (c.field_count == FIELDS
               && c.count == messages.len()
               && fields
                  .iter()
                  .zip(&c.fields)
                  .all(|(field, range)| *field == &c.frame[range.clone()])
               && first == &c.frame[c.message.clone()])
               .then(|| Arc::clone(&c.frame))
         })
      })
   {
      return frame;
   }
   let prefix_len = fields.iter().fold(prefix.len(), |n, field| {
      n.checked_add(bulk_len(field.len()))
         .expect("pub/sub frame length overflow")
   });
   let same_len = identical || messages.iter().all(|m| m.as_ref().len() == first.len());
   let total = if same_len {
      prefix_len
         .checked_add(bulk_len(first.len()))
         .and_then(|n| n.checked_mul(messages.len()))
         .expect("pub/sub frame length overflow")
   } else {
      messages.iter().fold(0usize, |n, message| {
         n.checked_add(prefix_len)
            .and_then(|n| n.checked_add(bulk_len(message.as_ref().len())))
            .expect("pub/sub frame length overflow")
      })
   };
   let mut arc = Arc::new_uninit_slice(total);
   let out = Arc::get_mut(&mut arc).unwrap();
   let mut at = 0;
   let mut ranges = [0..0, 0..0];
   if prefix_len <= 64 {
      let mut shared = [MaybeUninit::uninit(); 96];
      let mut used = 0;
      append(&mut shared, &mut used, prefix);
      for (index, field) in fields.iter().enumerate() {
         let start = used + int_len(field.len()) + 3;
         ranges[index] = start..start + field.len();
         bulk(&mut shared, &mut used, field);
      }
      if same_len {
         let mut h = [0; 24];
         let n = header(first.len(), &mut h);
         append(&mut shared, &mut used, &h[..n]);
      }
      // Only the prefix already written above is exposed as initialized bytes.
      let shared = unsafe { shared[..used].assume_init_ref() };
      for message in messages {
         append(out, &mut at, shared);
         if same_len {
            append(out, &mut at, message.as_ref());
            append(out, &mut at, b"\r\n");
         } else {
            bulk(out, &mut at, message.as_ref());
         }
         if identical {
            break;
         }
      }
   } else {
      for message in messages {
         append(out, &mut at, prefix);
         for (index, field) in fields.iter().enumerate() {
            let start = at + int_len(field.len()) + 3;
            if index < ranges.len() {
               ranges[index] = start..start + field.len();
            }
            bulk(out, &mut at, field);
         }
         bulk(out, &mut at, message.as_ref());
         if identical {
            break;
         }
      }
   }
   if identical {
      while at < total {
         let n = at.min(total - at);
         out.copy_within(..n, at);
         at += n;
      }
   }
   assert_eq!(at, total);
   // All fields and every repeated block are initialized before publication.
   let frame = unsafe { arc.assume_init() };
   if identical && frame.len() <= MAX_CACHED_FRAME_BYTES {
      let message_start = prefix_len + int_len(first.len()) + 3;
      BATCH_CACHE.with_borrow_mut(|cache| {
         *cache = Some(CachedBatch {
            fields: ranges,
            field_count: FIELDS,
            message: message_start..message_start + first.len(),
            count: messages.len(),
            frame: Arc::clone(&frame),
         });
      });
   }
   frame
}

pub fn encode_sub_reply(kind: &str, channel: &str, count: usize, out: &mut Vec<u8>) {
   out.reserve(32 + kind.len() + channel.len());
   out.extend_from_slice(b"*3\r\n");
   bulk_into(out, kind.as_bytes());
   bulk_into(out, channel.as_bytes());
   out.push(b':');
   push_usize(out, count);
   out.extend_from_slice(b"\r\n");
}

#[inline(always)]
pub(crate) fn bulk_into(out: &mut Vec<u8>, bytes: &[u8]) {
   let mut h = [0; 24];
   let n = header(bytes.len(), &mut h);
   out.extend_from_slice(&h[..n]);
   out.extend_from_slice(bytes);
   out.extend_from_slice(b"\r\n");
}

#[inline]
pub(crate) fn push_usize(out: &mut Vec<u8>, value: usize) {
   let mut digits = [0; 20];
   out.extend_from_slice(crate::utils::resp::format_usize_digits(value, &mut digits));
}
