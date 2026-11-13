use super::*;
use crate::storage::value::{FyroDB, StoreValue};
use std::io::IoSlice;

pub(super) const MIN_STREAM_BYTES: usize = 1024;
const BATCH: usize = 256;

fn owner_slot(
   entries: &[*const ()],
   seen: &[u16; BATCH * 2],
   pointer: *const (),
) -> (usize, Option<usize>) {
   let hash = ((pointer as usize >> 3) as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
   let mut slot = (hash >> (64 - (BATCH * 2).trailing_zeros())) as usize;
   while seen[slot] != 0 {
      let previous = usize::from(seen[slot] - 1);
      if entries[previous] == pointer {
         return (slot, Some(previous));
      }
      slot = (slot + 1) & (BATCH * 2 - 1);
   }
   (slot, None)
}

fn header(length: usize, out: &mut [u8; 24]) -> usize {
   let mut digits = [0u8; 20];
   let number = crate::utils::resp::format_usize_digits(length, &mut digits);
   out[0] = b'$';
   out[1..1 + number.len()].copy_from_slice(number);
   out[1 + number.len()..3 + number.len()].copy_from_slice(b"\r\n");
   number.len() + 3
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
   let prefix = buffered.len() - *offset;
   let written = loop {
      let mut slices = [IoSlice::new(&[]); BATCH * 3 + 1];
      slices[0] = IoSlice::new(&buffered[*offset..]);
      for (i, value) in values.iter().enumerate() {
         slices[i * 3 + 1] = IoSlice::new(&headers[i][..usize::from(lengths[i])]);
         slices[i * 3 + 2] = IoSlice::new(value);
         slices[i * 3 + 3] = IoSlice::new(b"\r\n");
      }
      match writer.write_vectored(&slices[..values.len() * 3 + 1]) {
         Ok(n) => break n,
         Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
         Err(_) => break 0,
      }
   };
   let mut remaining = if written >= prefix {
      buffered.clear();
      *offset = 0;
      written - prefix
   } else {
      *offset += written;
      0
   };
   buffered.reserve(body_bytes.saturating_sub(remaining));
   for (i, value) in values.iter().enumerate() {
      for part in [
         &headers[i][..usize::from(lengths[i])],
         *value,
         b"\r\n".as_slice(),
      ] {
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
/// Entries must stay pinned, and epoch must guard conn's store.
#[inline(never)]
pub(super) unsafe fn try_stream_get_run(
   conn: &mut Conn<'_>,
   keys: &[&[u8]],
   hashes: &[u64],
   entries: &[*const ()],
   epoch: &mut Option<customhash::ReadEpochGuard<'_>>,
) -> bool {
   let count = keys.len();
   if count == 0
      || count > BATCH
      || hashes.len() != count
      || entries.len() != count
      || epoch.is_none()
   {
      return false;
   }
   let mut reads: [Option<customhash::EntryReadGuard<'_, StoreValue>>; BATCH] =
      [const { None }; BATCH];
   // Owner indexes become header lengths after all guards are acquired.
   let mut lengths = [0u16; BATCH];
   let mut seen = [0u16; BATCH * 2];
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
   // Entry guards keep payloads immutable after releasing the global epoch.
   drop(epoch.take());
   let mut headers = [[0u8; 24]; BATCH];
   let mut values: [&[u8]; BATCH] = [&[]; BATCH];
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
         let mut written = 0;
         for slice in slices {
            let count = slice.len().min(self.limit - written);
            self.wire.extend_from_slice(&slice[..count]);
            written += count;
         }
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
