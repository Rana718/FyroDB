pub const HASH_SLOTS: u16 = 16_384;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Slot(pub u16);

impl Slot {
   pub const fn new(value: u16) -> Option<Self> {
      if value < HASH_SLOTS {
         Some(Self(value))
      } else {
         None
      }
   }

   pub const fn value(self) -> u16 {
      self.0
   }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotRange {
   pub start: Slot,
   pub end: Slot,
}

impl SlotRange {
   pub const fn new(start: Slot, end: Slot) -> Option<Self> {
      if start.0 <= end.0 {
         Some(Self { start, end })
      } else {
         None
      }
   }

   pub const fn contains(self, slot: Slot) -> bool {
      slot.0 >= self.start.0 && slot.0 <= self.end.0
   }
}

#[inline]
pub fn hash_slot(key: &[u8]) -> Slot {
   Slot(crc16(hashtag(key)) % HASH_SLOTS)
}

#[inline]
fn hashtag(key: &[u8]) -> &[u8] {
   let Some(open) = memchr::memchr(b'{', key) else {
      return key;
   };
   let start = open + 1;
   let Some(relative_end) = memchr::memchr(b'}', &key[start..]) else {
      return key;
   };
   let end = start + relative_end;
   if start == end { key } else { &key[start..end] }
}

#[inline]
fn crc16(bytes: &[u8]) -> u16 {
   // Slicing-by-8: `TABLES[n][i]` is the state after byte `i` followed by
   // `n` zero bytes, so eight input bytes resolve as eight independent
   // lookups instead of an eight-link dependency chain.
   let mut crc = 0u16;
   let (chunks, remainder) = bytes.as_chunks::<8>();
   for chunk in chunks {
      let head = crc ^ u16::from_be_bytes([chunk[0], chunk[1]]);
      crc = CRC16_TABLES[7][(head >> 8) as usize]
         ^ CRC16_TABLES[6][(head & 0xff) as usize]
         ^ CRC16_TABLES[5][chunk[2] as usize]
         ^ CRC16_TABLES[4][chunk[3] as usize]
         ^ CRC16_TABLES[3][chunk[4] as usize]
         ^ CRC16_TABLES[2][chunk[5] as usize]
         ^ CRC16_TABLES[1][chunk[6] as usize]
         ^ CRC16_TABLES[0][chunk[7] as usize];
   }
   for &byte in remainder {
      let index = ((crc >> 8) as u8 ^ byte) as usize;
      crc = (crc << 8) ^ CRC16_TABLES[0][index];
   }
   crc
}

const CRC16_TABLES: [[u16; 256]; 8] = build_crc16_tables();

const fn build_crc16_tables() -> [[u16; 256]; 8] {
   let mut tables = [[0u16; 256]; 8];
   let mut index = 0;
   while index < 256 {
      let mut crc = (index as u16) << 8;
      let mut bit = 0;
      while bit < 8 {
         crc = if crc & 0x8000 != 0 {
            (crc << 1) ^ 0x1021
         } else {
            crc << 1
         };
         bit += 1;
      }
      tables[0][index] = crc;
      index += 1;
   }
   let mut slice = 1;
   while slice < 8 {
      let mut i = 0;
      while i < 256 {
         let previous = tables[slice - 1][i];
         tables[slice][i] = (previous << 8) ^ tables[0][(previous >> 8) as usize];
         i += 1;
      }
      slice += 1;
   }
   tables
}

#[cfg(test)]
mod tests {
   use super::*;

   #[test]
   fn matches_redis_hash_tags_and_vectors() {
      assert_eq!(hash_slot(b"foo{bar}"), hash_slot(b"{bar}"));
      assert_eq!(
         hash_slot(b"{user1000}.following"),
         hash_slot(b"{user1000}.followers")
      );
      assert_ne!(hash_slot(b"foo{}bar"), hash_slot(b"{}"));
      assert_eq!(hash_slot(b"foo").value(), 12182);
      assert_eq!(hash_slot(b"bar").value(), 5061);
      assert_eq!(hash_slot(b"{user1000}.following").value(), 3443);
   }

   #[test]
   fn validates_slots_and_ranges() {
      assert!(Slot::new(HASH_SLOTS - 1).is_some());
      assert!(Slot::new(HASH_SLOTS).is_none());
      let range = SlotRange::new(Slot(10), Slot(20)).unwrap();
      assert!(range.contains(Slot(10)));
      assert!(range.contains(Slot(20)));
      assert!(!range.contains(Slot(21)));
      assert!(SlotRange::new(Slot(20), Slot(10)).is_none());
   }

   #[test]
   fn slicing_crc16_matches_the_bytewise_definition() {
      fn reference(bytes: &[u8]) -> u16 {
         let mut crc = 0u16;
         for &byte in bytes {
            let index = ((crc >> 8) as u8 ^ byte) as usize;
            crc = (crc << 8) ^ CRC16_TABLES[0][index];
         }
         crc
      }
      // Redis uses CRC-16/XMODEM here: poly 0x1021 with a zero initial state
      // (not CCITT-FALSE's 0xffff init), whose check value differs.
      assert_eq!(crc16(b"123456789"), 0x31c3);
      let data: Vec<u8> = (0..=255u8).cycle().take(600).collect();
      for length in 0..data.len() {
         assert_eq!(
            crc16(&data[..length]),
            reference(&data[..length]),
            "length {length}"
         );
      }
   }
}
