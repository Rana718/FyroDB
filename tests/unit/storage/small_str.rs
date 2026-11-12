#[test]
#[cfg(target_pointer_width = "64")]
fn database_values_keep_their_memory_budget() {
   use crate::storage::value::{FyroDB, SmallStr, StoreValue};
   assert!(std::mem::size_of::<SmallStr>() <= 24);
   assert!(std::mem::size_of::<FyroDB>() <= 24);
   assert!(std::mem::size_of::<StoreValue>() <= 32);
   assert!(std::mem::size_of::<customhash::Entry<StoreValue>>() <= 56);
}

#[test]
fn binary_values_round_trip_across_inline_and_heap_storage() {
   for size in [0, 1, 23, 24, 4096] {
      let bytes: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
      let value = super::SmallStr::from_slice(&bytes);
      assert_eq!(value.as_bytes(), bytes);
      assert_eq!(value.clone().into_bytes(), bytes);
      assert_eq!(
         super::SmallStr::from_bytes(bytes.clone()).into_bytes(),
         bytes
      );
   }
}

#[test]
fn raw_bulk_replies_preserve_binary_values_and_destination_bounds() {
   for size in [
      0,
      1,
      9,
      10,
      23,
      24,
      64,
      99,
      100,
      999,
      1000,
      65536,
      1024 * 1024,
   ] {
      let bytes: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
      let value = super::SmallStr::from_slice(&bytes);
      let capacity = (size + 32).max(64);
      let mut destination = vec![0xa5; capacity + 16];
      let used = unsafe { value.write_resp_raw(destination.as_mut_ptr()) };
      let mut expected = Vec::new();
      crate::utils::resp::write_bulk_bytes(&mut expected, &bytes);
      assert_eq!(&destination[..used], expected);
      assert!(destination[capacity..].iter().all(|b| *b == 0xa5));
   }
}

use super::SmallStr;

#[test]
fn clone_is_an_independent_copy() {
   let long = "0123456789abcdefghijklmnopqrstuvwxyz";
   let a = SmallStr::new(long);
   let b = a.clone();
   assert_eq!(a.as_str(), long);
   assert_eq!(b.as_str(), long);
   // Heap variant must not alias: two owners of one box would double-free.
   if long.len() > super::SMALL_STR_CAP {
      assert_ne!(
         a.as_str().as_ptr(),
         b.as_str().as_ptr(),
         "cloned heap variant aliases the original allocation"
      );
   }
   let mut c = a.clone();
   c.push_str("x");
   assert_eq!(a.as_str(), long);
   assert_eq!(c.as_str(), "0123456789abcdefghijklmnopqrstuvwxyzx");
}

#[test]
fn layout_is_24_bytes_with_23_inline() {
   assert_eq!(std::mem::size_of::<SmallStr>(), 24);
   let s = SmallStr::new("01234567890123456789012");
   assert_eq!(s.len(), 23);
   assert_ne!(s.len, super::Tag::Heap);
   let h = SmallStr::new("012345678901234567890123");
   assert_eq!(h.len, super::Tag::Heap);
   assert_eq!(h.as_str().len(), 24);
}

#[test]
fn push_str_stays_inline_when_it_fits() {
   let mut s = SmallStr::new("hello");
   s.push_str(" world"); // 11 bytes total, fits inline
   assert_eq!(s.as_str(), "hello world");
}

#[test]
fn same_size_heap_overwrites_reuse_storage_and_preserve_clones() {
   for size in (24..=258).chain([512, 1024, 4096, 65536]) {
      let mut value = SmallStr::from_slice(&vec![b'a'; size]);
      let snapshot = value.clone();
      let allocation = value.as_bytes().as_ptr();
      for byte in [0, 255, b'z'] {
         let replacement: Vec<_> = (0..size)
            .map(|offset| byte.wrapping_add(offset as u8))
            .collect();
         value.overwrite_bytes(&replacement);
         assert_eq!(value.as_bytes(), replacement);
         assert_eq!(value.as_bytes().as_ptr(), allocation);
         assert_eq!(snapshot.as_bytes(), vec![b'a'; size]);
         let mut reply = Vec::new();
         value.write_resp(&mut reply);
         let mut expected = format!("${size}\r\n").into_bytes();
         expected.extend_from_slice(&replacement);
         expected.extend_from_slice(b"\r\n");
         assert_eq!(reply, expected);
      }
   }
}

#[test]
fn overwrites_cross_inline_heap_and_size_boundaries() {
   let mut value = SmallStr::default();
   for size in [0, 23, 24, 64, 4096, 24, 23, 1, 0, 65536, 64] {
      let bytes: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
      value.overwrite_bytes(&bytes);
      assert_eq!(value.len(), size);
      assert_eq!(value.as_bytes(), bytes);
      assert_eq!(value.clone().into_bytes(), bytes);
   }
}

#[test]
fn binary_replies_are_safe_with_empty_and_tightly_sized_output_buffers() {
   for size in 0..=24 {
      let bytes = vec![255; size];
      let value = SmallStr::from_slice(&bytes);
      let mut expected = format!("${size}\r\n").into_bytes();
      expected.extend_from_slice(&bytes);
      expected.extend_from_slice(b"\r\n");
      for prefix in [0, 7, 15] {
         let mut output = Vec::with_capacity(prefix + expected.len());
         output.resize(prefix, b'x');
         value.write_resp(&mut output);
         assert_eq!(&output[..prefix], vec![b'x'; prefix]);
         assert_eq!(&output[prefix..], expected);
      }
   }
}

#[test]
fn binary_growth_preserves_bytes_and_zero_fills_inline_and_heap_tails() {
   let mut value = SmallStr::from_slice(&[255, 128]);
   for size in [3, 23, 24, 64, 4096] {
      value.ensure_len(size);
      assert_eq!(&value.as_bytes()[..2], &[255, 128]);
      assert!(value.as_bytes()[2..].iter().all(|byte| *byte == 0));
      if size <= 23 {
         assert_ne!(value.len, super::Tag::Heap);
      }
   }
}

#[test]
fn appending_to_binary_values_preserves_the_prefix_and_utf8_suffix_bytes() {
   for size in [2, 23, 24, 4096] {
      let mut value = SmallStr::from_slice(&vec![255; size]);
      value.push_str("é");
      let mut expected = vec![255; size];
      expected.extend_from_slice("é".as_bytes());
      assert_eq!(value.into_bytes(), expected);
   }
}
