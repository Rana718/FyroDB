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

pub fn hash_slot(key: &[u8]) -> Slot {
    Slot(crc16(hashtag(key)) % HASH_SLOTS)
}

fn hashtag(key: &[u8]) -> &[u8] {
    let Some(open) = key.iter().position(|&byte| byte == b'{') else {
        return key;
    };
    let start = open + 1;
    let Some(relative_end) = key[start..].iter().position(|&byte| byte == b'}') else {
        return key;
    };
    let end = start + relative_end;
    if start == end { key } else { &key[start..end] }
}

fn crc16(bytes: &[u8]) -> u16 {
    let mut crc = 0u16;
    for &byte in bytes {
        let index = ((crc >> 8) as u8 ^ byte) as usize;
        crc = (crc << 8) ^ CRC16_TABLE[index];
    }
    crc
}

const CRC16_TABLE: [u16; 256] = build_crc16_table();

const fn build_crc16_table() -> [u16; 256] {
    let mut table = [0u16; 256];
    let mut index = 0;
    while index < table.len() {
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
        table[index] = crc;
        index += 1;
    }
    table
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
}
