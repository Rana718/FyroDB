use super::*;
use crate::storage::value::{FyroDB, StoreValue};
use std::io::IoSlice;

pub(super) const MIN_STREAM_BYTES: usize = 1024;
pub(super) const DIRECT_SEND_MIN: usize = 32 * 1024;
const BATCH: usize = 256;
const EAGER_DRAIN_CALLS_MIN: usize = 32;
const EAGER_DRAIN_CALLS_MAX: usize = 256;
const EAGER_DRAIN_BYTES_PER_CALL: usize = 64 * 1024;

#[inline]
fn eager_drain_calls(total_bytes: usize) -> usize {
   (total_bytes / EAGER_DRAIN_BYTES_PER_CALL + EAGER_DRAIN_CALLS_MIN).min(EAGER_DRAIN_CALLS_MAX)
}

#[inline(always)]
fn owner_slot<const SEEN_LEN: usize>(
   entries: &[*const ()],
   seen: &[u16; SEEN_LEN],
   pointer: *const (),
) -> (usize, Option<usize>) {
   let hash = ((pointer as usize >> 3) as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
   let mut slot = (hash >> (64 - SEEN_LEN.trailing_zeros())) as usize;
   let mask = SEEN_LEN - 1;
   while seen[slot] != 0 {
      let previous = usize::from(seen[slot] - 1);
      if entries[previous] == pointer {
         return (slot, Some(previous));
      }
      slot = (slot + 1) & mask;
   }
   (slot, None)
}

#[inline(always)]
fn header(length: usize, out: &mut [u8; 24]) -> usize {
   crate::utils::resp::format_bulk_header(length, out)
}

#[inline]
fn write_vectored_bounded<W: Write>(
   writer: &mut W,
   slices: &mut [IoSlice<'_>],
   max_calls: usize,
) -> usize {
   let mut pending = slices;
   let mut written = 0;
   let mut calls = 0;
   while calls < max_calls && !pending.is_empty() {
      match writer.write_vectored(pending) {
         Ok(0) => break,
         Ok(n) => {
            written += n;
            calls += 1;
            IoSlice::advance_slices(&mut pending, n);
         }
         Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
         Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
         Err(_) => break,
      }
   }
   written
}

#[inline(never)]
fn send_batch<W: Write>(
   writer: &mut W,
   buffered: &mut Vec<u8>,
   offset: &mut usize,
   headers: &[[u8; 24]],
   lengths: &[u16],
   values: &[&[u8]],
) {
   let body_bytes = lengths
      .iter()
      .zip(values)
      .try_fold(0usize, |total, (length, value)| {
         total
            .checked_add(usize::from(*length))?
            .checked_add(value.len())?
            .checked_add(2)
      })
      .expect("reply length overflow");
   send_frames(writer, buffered, offset, values.len(), body_bytes, |i| {
      (&headers[i][..usize::from(lengths[i])], values[i])
   });
}

pub(super) fn send_repeated_value<W: Write>(
   writer: &mut W,
   buffered: &mut Vec<u8>,
   offset: &mut usize,
   value: &[u8],
   count: usize,
) {
   let mut header = [0; 24];
   let n = crate::utils::resp::format_bulk_header(value.len(), &mut header);
   let body = (n + value.len() + 2)
      .checked_mul(count)
      .expect("reply length overflow");
   send_frames(writer, buffered, offset, count, body, |_| {
      (&header[..n], value)
   });
}

fn send_frames<'a, W: Write>(
   writer: &mut W,
   buffered: &mut Vec<u8>,
   offset: &mut usize,
   count: usize,
   body_bytes: usize,
   frame: impl Fn(usize) -> (&'a [u8], &'a [u8]),
) {
   let prefix = buffered.len() - *offset;
   let total = prefix.saturating_add(body_bytes);
   let max_calls = if total >= DIRECT_SEND_MIN {
      eager_drain_calls(total)
   } else {
      EAGER_DRAIN_CALLS_MIN
   };
   #[inline(always)]
   fn fill_slices<'a, 'data: 'a>(
      slices: &mut [IoSlice<'a>],
      prefix: Option<&'a [u8]>,
      count: usize,
      frame: impl Fn(usize) -> (&'data [u8], &'data [u8]),
   ) -> usize {
      let mut idx = 0;
      if let Some(p) = prefix {
         slices[idx] = IoSlice::new(p);
         idx += 1;
      }
      for i in 0..count {
         let (header, value) = frame(i);
         slices[idx] = IoSlice::new(header);
         slices[idx + 1] = IoSlice::new(value);
         slices[idx + 2] = IoSlice::new(b"\r\n");
         idx += 3;
      }
      idx
   }

   let prefix_slice = if prefix > 0 {
      Some(&buffered[*offset..])
   } else {
      None
   };
   let written = if count <= 16 {
      let mut slices = [IoSlice::new(&[]); 16 * 3 + 1];
      let slice_idx = fill_slices(&mut slices, prefix_slice, count, &frame);
      write_vectored_bounded(writer, &mut slices[..slice_idx], max_calls)
   } else if count <= 32 {
      let mut slices = [IoSlice::new(&[]); 32 * 3 + 1];
      let slice_idx = fill_slices(&mut slices, prefix_slice, count, &frame);
      write_vectored_bounded(writer, &mut slices[..slice_idx], max_calls)
   } else {
      let mut slices = [IoSlice::new(&[]); BATCH * 3 + 1];
      let slice_idx = fill_slices(&mut slices, prefix_slice, count, &frame);
      write_vectored_bounded(writer, &mut slices[..slice_idx], max_calls)
   };
   if written == total {
      buffered.clear();
      *offset = 0;
      if buffered.capacity() > RETAINED_WRITE_BUFFER {
         buffered.shrink_to(RETAINED_WRITE_BUFFER);
      }
      return;
   }
   let mut remaining = if written >= prefix {
      buffered.clear();
      *offset = 0;
      written - prefix
   } else {
      *offset += written;
      0
   };
   buffered.reserve(body_bytes.saturating_sub(remaining));
   for i in 0..count {
      let (header, value) = frame(i);
      for part in [header, value, b"\r\n".as_slice()] {
         if remaining >= part.len() {
            remaining -= part.len();
         } else {
            buffered.extend_from_slice(&part[remaining..]);
            remaining = 0;
         }
      }
   }
   if buffered.is_empty() && buffered.capacity() > RETAINED_WRITE_BUFFER {
      buffered.shrink_to(RETAINED_WRITE_BUFFER);
   }
}

/// # Safety
/// Entries must stay pinned for duration of call (covered by EBR guard).
#[inline(never)]
pub(super) unsafe fn try_stream_get_run(
   conn: &mut Conn<'_>,
   keys: &[&[u8]],
   hashes: &[u64],
   entries: &[*const ()],
   epoch: &mut Option<customhash::ReadEpochGuard<'_>>,
) -> bool {
   let count = keys.len();
   if count == 0 || count > BATCH || hashes.len() != count || entries.len() != count {
      return false;
   }
   if epoch.is_some() && count <= 32 {
      let mut headers = [[0u8; 24]; 32];
      let mut values: [&[u8]; 32] = [&[]; 32];
      let mut lengths = [0u16; 32];
      let mut valid = true;
      for i in 0..count {
         let Some(entry) = (unsafe { entries[i].cast::<customhash::Entry<StoreValue>>().as_ref() })
         else {
            valid = false;
            break;
         };
         let expected = ((hashes[i] as u32 as u64) << 32) | customhash::STATE_OCCUPIED;
         if entry.state.load(Ordering::Relaxed)
            & (customhash::STATE_MATCH_MASK & !customhash::STATE_LOCK)
            != expected
            || !entry.key.eq_bytes(keys[i])
         {
            valid = false;
            break;
         }
         let val = unsafe { (*entry.value.get()).assume_init_ref() };
         if val.expires_ms != 0 && val.is_expired() {
            valid = false;
            break;
         }
         let FyroDB::String(value) = &val.value else {
            valid = false;
            break;
         };
         values[i] = value.as_bytes();
         lengths[i] = header(values[i].len(), &mut headers[i]) as u16;
      }
      if valid {
         send_batch(
            &mut conn.stream,
            &mut conn.parser.wbuf,
            &mut conn.write_offset,
            &headers[..count],
            &lengths[..count],
            &values[..count],
         );
         return true;
      }
   }
   #[inline(always)]
   unsafe fn stream_get_with_reads<const CAP: usize, const SEEN_CAP: usize>(
      conn: &mut Conn<'_>,
      epoch: &mut Option<customhash::ReadEpochGuard<'_>>,
      entries: &[*const ()],
      hashes: &[u64],
      keys: &[&[u8]],
      count: usize,
   ) -> bool {
      let mut reads: [Option<customhash::EntryReadGuard<'_, StoreValue>>; CAP] =
         [const { None }; CAP];
      let mut lengths = [0u16; CAP];
      let mut seen = [0u16; SEEN_CAP];
      for i in 0..count {
         let (slot, previous) = owner_slot(entries, &seen, entries[i]);
         if let Some(previous) = previous {
            if hashes[i] != hashes[previous] || keys[i] != keys[previous] {
               return false;
            }
            lengths[i] = previous as u16;
            continue;
         }
         let Some(entry) = (unsafe { entries[i].cast::<customhash::Entry<StoreValue>>().as_ref() })
         else {
            return false;
         };
         let expected = ((hashes[i] as u32 as u64) << 32) | customhash::STATE_OCCUPIED;
         if entry.state.load(Ordering::Relaxed)
            & (customhash::STATE_MATCH_MASK & !customhash::STATE_LOCK)
            != expected
            || !entry.key.eq_bytes(keys[i])
         {
            return false;
         }
         let Some(read) = entry.read() else {
            return false;
         };
         if read.is_expired() || !matches!(read.value, FyroDB::String(_)) {
            return false;
         }
         reads[i] = Some(read);
         lengths[i] = i as u16;
         seen[slot] = (i + 1) as u16;
      }
      drop(epoch.take());
      let mut headers = [[0u8; 24]; CAP];
      let mut values: [&[u8]; CAP] = [&[]; CAP];
      for i in 0..count {
         let read = reads[usize::from(lengths[i])].as_ref().unwrap();
         let FyroDB::String(value) = &read.value else {
            unreachable!()
         };
         values[i] = value.as_bytes();
         lengths[i] = header(values[i].len(), &mut headers[i]) as u16;
      }
      send_batch(
         &mut conn.stream,
         &mut conn.parser.wbuf,
         &mut conn.write_offset,
         &headers[..count],
         &lengths[..count],
         &values[..count],
      );
      true
   }

   if count <= 16 {
      return unsafe { stream_get_with_reads::<16, 32>(conn, epoch, entries, hashes, keys, count) };
   }
   unsafe {
      stream_get_with_reads::<BATCH, { BATCH * 2 }>(conn, epoch, entries, hashes, keys, count)
   }
}

#[cfg(test)]
mod tests {
   use super::*;

   #[test]
   fn ownership_index_resolves_collisions_and_reuses_all_aliases() {
      let storage = [0u8; BATCH * 16];
      let entries: Vec<_> = (0..BATCH)
         .map(|i| storage.as_ptr().wrapping_add(i * 16).cast())
         .collect();
      let mut seen = [0u16; BATCH * 2];
      for (i, &pointer) in entries.iter().enumerate() {
         let (slot, previous) = owner_slot(&entries, &seen, pointer);
         assert_eq!(previous, None);
         seen[slot] = (i + 1) as u16;
      }
      for (i, &pointer) in entries.iter().enumerate().rev() {
         assert_eq!(owner_slot(&entries, &seen, pointer).1, Some(i));
      }
      assert_eq!(owner_slot(&entries, &seen, std::ptr::null()).1, None);
   }

   struct ShortWriter {
      limit: usize,
      error: Option<io::ErrorKind>,
      interrupt: bool,
      wire: Vec<u8>,
   }

   impl Write for ShortWriter {
      fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
         self.write_vectored(&[IoSlice::new(bytes)])
      }
      fn flush(&mut self) -> io::Result<()> {
         Ok(())
      }
      fn write_vectored(&mut self, slices: &[IoSlice<'_>]) -> io::Result<usize> {
         if std::mem::take(&mut self.interrupt) {
            return Err(io::ErrorKind::Interrupted.into());
         }
         if let Some(error) = self.error {
            return Err(error.into());
         }
         let before = self.wire.len();
         let mut written = 0;
         for slice in slices {
            let count = slice.len().min(self.limit - written);
            self.wire.extend_from_slice(&slice[..count]);
            written += count;
         }
         assert_eq!(self.wire.len() - before, written);
         Ok(written)
      }
   }

   #[test]
   fn vectored_replies_preserve_every_partial_write_boundary() {
      let values: [&[u8]; 3] = [b"a\x00\xff\r\nb", b"", b"the last value"];
      let mut headers = [[0; 24]; 3];
      let lengths: Vec<_> = values
         .iter()
         .zip(&mut headers)
         .map(|(v, h)| header(v.len(), h) as u16)
         .collect();
      let prefix = b":1\r\n+PONG\r\n";
      let mut response = Vec::new();
      for value in values {
         crate::utils::resp::write_bulk_bytes(&mut response, value);
      }
      for offset in [0, 4, prefix.len()] {
         let expected = [prefix[offset..].to_vec(), response.clone()].concat();
         for limit in 0..=expected.len() {
            let mut writer = ShortWriter {
               limit,
               error: None,
               interrupt: true,
               wire: Vec::new(),
            };
            let mut buffered = prefix.to_vec();
            let mut at = offset;
            send_batch(
               &mut writer,
               &mut buffered,
               &mut at,
               &headers,
               &lengths,
               &values,
            );
            writer.wire.extend_from_slice(&buffered[at..]);
            assert_eq!(writer.wire, expected, "offset={offset} limit={limit}");
         }
      }
   }

   #[test]
   fn repeated_owned_replies_preserve_every_prefix_and_partial_write_boundary() {
      let prefix = b":1\r\n+PONG\r\n";
      let mut reply = Vec::new();
      crate::utils::resp::write_bulk_bytes(&mut reply, b"\0\xff\r\nbody");
      for count in [1, 2, 7, 256] {
         for offset in [0, 4, prefix.len()] {
            let expected = [prefix[offset..].to_vec(), reply.repeat(count)].concat();
            for limit in 0..=expected.len() {
               let mut writer = ShortWriter {
                  limit,
                  error: None,
                  interrupt: true,
                  wire: Vec::new(),
               };
               let mut buffered = prefix.to_vec();
               let mut at = offset;
               send_repeated_value(
                  &mut writer,
                  &mut buffered,
                  &mut at,
                  b"\0\xff\r\nbody",
                  count,
               );
               writer.wire.extend_from_slice(&buffered[at..]);
               assert_eq!(
                  writer.wire, expected,
                  "count={count} offset={offset} limit={limit}"
               );
            }
         }
      }
      for error in [io::ErrorKind::WouldBlock, io::ErrorKind::Other] {
         let mut writer = ShortWriter {
            limit: 0,
            error: Some(error),
            interrupt: false,
            wire: Vec::new(),
         };
         let mut buffered = Vec::new();
         let mut offset = 0;
         send_repeated_value(
            &mut writer,
            &mut buffered,
            &mut offset,
            b"\0\xff\r\nbody",
            256,
         );
         assert_eq!(buffered, reply.repeat(256));
      }
   }

   #[test]
   fn repeated_partial_writes_keep_only_the_unsent_repetitions() {
      let mut reply = Vec::new();
      crate::utils::resp::write_bulk_bytes(&mut reply, &vec![255; 8192]);
      let mut writer = ShortWriter {
         limit: reply.len() * 2 + 3,
         error: None,
         interrupt: false,
         wire: Vec::new(),
      };
      let mut buffered = Vec::new();
      let mut offset = 0;
      send_repeated_value(&mut writer, &mut buffered, &mut offset, &[255; 8192], 17);
      assert!(buffered.len() < reply.len() * 17);
      assert!(offset < reply.len());
      writer.wire.extend_from_slice(&buffered[offset..]);
      assert_eq!(writer.wire, reply.repeat(17));
   }

   #[test]
   fn vectored_errors_and_blocked_payloads_keep_owned_replies() {
      for error in [
         io::ErrorKind::WouldBlock,
         io::ErrorKind::Other,
         io::ErrorKind::InvalidInput,
      ] {
         let mut value = vec![255; 65536];
         let mut expected = b"+OK\r\n".to_vec();
         crate::utils::resp::write_bulk_bytes(&mut expected, &value);
         let mut headers = [[0; 24]; 1];
         let lengths = [header(value.len(), &mut headers[0]) as u16];
         let mut buffered = b"+OK\r\n".to_vec();
         let mut offset = 0;
         let mut writer = ShortWriter {
            limit: 0,
            error: Some(error),
            interrupt: false,
            wire: Vec::new(),
         };
         send_batch(
            &mut writer,
            &mut buffered,
            &mut offset,
            &headers,
            &lengths,
            &[&value],
         );
         value.fill(0);
         assert_eq!(&buffered[offset..], expected);
         assert!(writer.wire.is_empty());
      }
   }

   #[test]
   fn completed_large_replies_need_no_output_allocation_and_reclaim_old_buffers() {
      let value = vec![b'x'; 1024 * 1024];
      let mut headers = [[0; 24]; 1];
      let lengths = [header(value.len(), &mut headers[0]) as u16];
      for capacity in [0, 2 * 1024 * 1024] {
         let mut buffered = Vec::with_capacity(capacity);
         let mut offset = 0;
         let mut writer = ShortWriter {
            limit: usize::MAX,
            error: None,
            interrupt: false,
            wire: Vec::new(),
         };
         send_batch(
            &mut writer,
            &mut buffered,
            &mut offset,
            &headers,
            &lengths,
            &[&value],
         );
         assert!(buffered.is_empty());
         assert_eq!(offset, 0);
         assert!(buffered.capacity() <= RETAINED_WRITE_BUFFER);
         if capacity == 0 {
            assert_eq!(buffered.capacity(), 0);
         }
         let mut expected = Vec::new();
         crate::utils::resp::write_bulk_bytes(&mut expected, &value);
         assert_eq!(writer.wire, expected);
      }
   }
}
