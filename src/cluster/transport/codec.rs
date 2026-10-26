use std::io::{self, IoSlice, Read, Write};

const MAGIC: [u8; 4] = *b"FYRC";
const VERSION: u8 = 1;
const HEADER_LEN: usize = 40;
/// Header plus the length/checksum trailer: one contiguous 48-byte prefix.
const PREFIX_LEN: usize = HEADER_LEN + 8;
pub const DEFAULT_MAX_PAYLOAD: usize = 16 * 1024 * 1024;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
   Hello = 1,
   Ping = 2,
   Pong = 3,
   Topology = 4,
   CommandForward = 5,
   CommandReply = 6,
   ReplicationBegin = 7,
   ReplicationEntry = 8,
   ReplicationAck = 9,
   ReplicationSnapshot = 10,
   ReplicationFinish = 11,
   FailureReport = 12,
   FailureConfirm = 13,
   MigrateBegin = 14,
   MigrateChunk = 15,
   MigrateFinish = 16,
}

impl TryFrom<u8> for MessageType {
   type Error = ProtocolError;

   fn try_from(value: u8) -> Result<Self, Self::Error> {
      Ok(match value {
         1 => Self::Hello,
         2 => Self::Ping,
         3 => Self::Pong,
         4 => Self::Topology,
         5 => Self::CommandForward,
         6 => Self::CommandReply,
         7 => Self::ReplicationBegin,
         8 => Self::ReplicationEntry,
         9 => Self::ReplicationAck,
         10 => Self::ReplicationSnapshot,
         11 => Self::ReplicationFinish,
         12 => Self::FailureReport,
         13 => Self::FailureConfirm,
         14 => Self::MigrateBegin,
         15 => Self::MigrateChunk,
         16 => Self::MigrateFinish,
         other => return Err(ProtocolError::UnknownMessage(other)),
      })
   }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
   pub message_type: MessageType,
   pub flags: u16,
   pub request_id: u64,
   pub source_id: u64,
   pub target_id: u64,
   pub epoch: u64,
   pub payload: Vec<u8>,
}

#[derive(Debug)]
pub enum ProtocolError {
   Io(io::Error),
   BadMagic,
   UnsupportedVersion(u8),
   UnknownMessage(u8),
   PayloadTooLarge { length: usize, maximum: usize },
   BadChecksum,
   InvalidHandshake,
}

impl std::fmt::Display for ProtocolError {
   fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      match self {
         Self::Io(error) => write!(f, "cluster transport I/O error: {error}"),
         Self::BadMagic => f.write_str("invalid cluster frame magic"),
         Self::UnsupportedVersion(version) => {
            write!(f, "unsupported cluster protocol version {version}")
         }
         Self::UnknownMessage(message) => write!(f, "unknown cluster message type {message}"),
         Self::PayloadTooLarge { length, maximum } => {
            write!(f, "cluster payload {length} exceeds maximum {maximum}")
         }
         Self::BadChecksum => f.write_str("cluster frame checksum mismatch"),
         Self::InvalidHandshake => f.write_str("invalid cluster peer handshake"),
      }
   }
}

impl std::error::Error for ProtocolError {}

impl From<io::Error> for ProtocolError {
   fn from(value: io::Error) -> Self {
      Self::Io(value)
   }
}

#[derive(Debug, Clone)]
pub struct FrameCodec {
   max_payload: usize,
}

impl Default for FrameCodec {
   fn default() -> Self {
      Self {
         max_payload: DEFAULT_MAX_PAYLOAD,
      }
   }
}

impl FrameCodec {
   pub fn new(max_payload: usize) -> Self {
      Self { max_payload }
   }

   pub fn write_frame(&self, writer: &mut impl Write, frame: &Frame) -> Result<(), ProtocolError> {
      let prefix = self.encode_prefix(frame)?;
      let mut slices = [IoSlice::new(&prefix), IoSlice::new(&frame.payload)];
      write_all_slices(writer, &mut slices)?;
      Ok(())
   }

   /// Encode a frame's 48-byte length/checksum prefix.
   fn encode_prefix(&self, frame: &Frame) -> Result<[u8; PREFIX_LEN], ProtocolError> {
      if frame.payload.len() > self.max_payload {
         return Err(ProtocolError::PayloadTooLarge {
            length: frame.payload.len(),
            maximum: self.max_payload,
         });
      }
      let mut prefix = [0u8; PREFIX_LEN];
      prefix[0..4].copy_from_slice(&MAGIC);
      prefix[4] = VERSION;
      prefix[5] = frame.message_type as u8;
      prefix[6..8].copy_from_slice(&frame.flags.to_be_bytes());
      prefix[8..16].copy_from_slice(&frame.request_id.to_be_bytes());
      prefix[16..24].copy_from_slice(&frame.source_id.to_be_bytes());
      prefix[24..32].copy_from_slice(&frame.target_id.to_be_bytes());
      prefix[32..40].copy_from_slice(&frame.epoch.to_be_bytes());
      prefix[40..44].copy_from_slice(&(frame.payload.len() as u32).to_be_bytes());
      prefix[44..48].copy_from_slice(&checksum(&frame.payload).to_be_bytes());
      Ok(prefix)
   }

   /// Write many frames in one vectored syscall. `scratch` holds encoded
   /// prefixes so the iovecs stay valid for the duration of the write.
   pub fn write_frames(
      &self,
      writer: &mut impl Write,
      frames: &[Frame],
      scratch: &mut Vec<[u8; PREFIX_LEN]>,
   ) -> Result<(), ProtocolError> {
      scratch.clear();
      scratch.reserve(frames.len());
      let mut slices = Vec::with_capacity(frames.len() * 2);
      for frame in frames {
         scratch.push(self.encode_prefix(frame)?);
      }
      for (prefix, frame) in scratch.iter().zip(frames.iter()) {
         slices.push(IoSlice::new(prefix));
         slices.push(IoSlice::new(&frame.payload));
      }
      write_all_slices(writer, &mut slices)?;
      Ok(())
   }

   pub fn read_frame(&self, reader: &mut impl Read) -> Result<Frame, ProtocolError> {
      let mut prefix = [0u8; PREFIX_LEN];
      reader.read_exact(&mut prefix)?;
      if prefix[0..4] != MAGIC {
         return Err(ProtocolError::BadMagic);
      }
      if prefix[4] != VERSION {
         return Err(ProtocolError::UnsupportedVersion(prefix[4]));
      }
      let message_type = MessageType::try_from(prefix[5])?;
      let length = u32::from_be_bytes(prefix[40..44].try_into().unwrap()) as usize;
      if length > self.max_payload {
         return Err(ProtocolError::PayloadTooLarge {
            length,
            maximum: self.max_payload,
         });
      }
      let expected_checksum = u32::from_be_bytes(prefix[44..48].try_into().unwrap());
      let mut payload = vec![0u8; length];
      reader.read_exact(&mut payload)?;
      if checksum(&payload) != expected_checksum {
         return Err(ProtocolError::BadChecksum);
      }
      Ok(Frame {
         message_type,
         flags: u16::from_be_bytes(prefix[6..8].try_into().unwrap()),
         request_id: u64::from_be_bytes(prefix[8..16].try_into().unwrap()),
         source_id: u64::from_be_bytes(prefix[16..24].try_into().unwrap()),
         target_id: u64::from_be_bytes(prefix[24..32].try_into().unwrap()),
         epoch: u64::from_be_bytes(prefix[32..40].try_into().unwrap()),
         payload,
      })
   }
}

/// Eight slicing-by-8 tables for IEEE 802.3 CRC32 calculation.
const CRC32_TABLES: [[u32; 256]; 8] = build_crc32_tables();

const fn build_crc32_tables() -> [[u32; 256]; 8] {
   let mut tables = [[0u32; 256]; 8];
   let mut index = 0;
   while index < 256 {
      let mut crc = index as u32;
      let mut bit = 0;
      while bit < 8 {
         crc = if crc & 1 != 0 {
            (crc >> 1) ^ 0xedb8_8320
         } else {
            crc >> 1
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
         tables[slice][i] = (previous >> 8) ^ tables[0][(previous & 0xff) as usize];
         i += 1;
      }
      slice += 1;
   }
   tables
}

#[inline]
fn checksum(bytes: &[u8]) -> u32 {
   let mut crc = !0u32;
   let (chunks, remainder) = bytes.as_chunks::<8>();
   for chunk in chunks {
      let low = u32::from_le_bytes(chunk[0..4].try_into().unwrap()) ^ crc;
      let high = u32::from_le_bytes(chunk[4..8].try_into().unwrap());
      crc = CRC32_TABLES[7][(low & 0xff) as usize]
         ^ CRC32_TABLES[6][((low >> 8) & 0xff) as usize]
         ^ CRC32_TABLES[5][((low >> 16) & 0xff) as usize]
         ^ CRC32_TABLES[4][(low >> 24) as usize]
         ^ CRC32_TABLES[3][(high & 0xff) as usize]
         ^ CRC32_TABLES[2][((high >> 8) & 0xff) as usize]
         ^ CRC32_TABLES[1][((high >> 16) & 0xff) as usize]
         ^ CRC32_TABLES[0][(high >> 24) as usize];
   }
   for &byte in remainder {
      crc = (crc >> 8) ^ CRC32_TABLES[0][((crc ^ u32::from(byte)) & 0xff) as usize];
   }
   !crc
}

/// writev with partial-write handling; stable counterpart of the nightly
/// `write_all_vectored`.
pub(crate) fn write_all_slices(
   writer: &mut impl Write,
   mut bufs: &mut [IoSlice<'_>],
) -> io::Result<()> {
   loop {
      let skip = bufs.iter().take_while(|b| b.is_empty()).count();
      bufs = &mut bufs[skip..];
      if bufs.is_empty() {
         return Ok(());
      }
      match writer.write_vectored(bufs) {
         Ok(0) => {
            return Err(io::Error::new(
               io::ErrorKind::WriteZero,
               "failed to write whole cluster frame",
            ));
         }
         Ok(mut n) => {
            let mut drop = 0;
            for b in bufs.iter() {
               if n >= b.len() {
                  n -= b.len();
                  drop += 1;
               } else {
                  break;
               }
            }
            bufs = &mut bufs[drop..];
            if n > 0
               && let Some(first) = bufs.first_mut()
            {
               IoSlice::advance(first, n);
            }
         }
         Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
         Err(error) => return Err(error),
      }
   }
}

#[cfg(test)]
mod tests {
   use super::*;

   fn frame() -> Frame {
      Frame {
         message_type: MessageType::Ping,
         flags: 7,
         request_id: 11,
         source_id: 12,
         target_id: 13,
         epoch: 14,
         payload: b"hello".to_vec(),
      }
   }

   #[test]
   fn round_trip_preserves_frame() {
      let codec = FrameCodec::new(1024);
      let mut bytes = Vec::new();
      codec.write_frame(&mut bytes, &frame()).unwrap();
      assert_eq!(codec.read_frame(&mut bytes.as_slice()).unwrap(), frame());
   }

   #[test]
   fn rejects_oversized_and_corrupt_payloads() {
      let codec = FrameCodec::new(4);
      assert!(matches!(
         codec.write_frame(&mut Vec::new(), &frame()),
         Err(ProtocolError::PayloadTooLarge { .. })
      ));
      let codec = FrameCodec::new(1024);
      let mut bytes = Vec::new();
      codec.write_frame(&mut bytes, &frame()).unwrap();
      *bytes.last_mut().unwrap() ^= 1;
      assert!(matches!(
         codec.read_frame(&mut bytes.as_slice()),
         Err(ProtocolError::BadChecksum)
      ));
   }

   /// The slicing-by-8 tables must reproduce the byte-at-a-time CRC32 that
   /// existing peers and on-disk frames were checksummed with.
   #[test]
   fn slicing_checksum_matches_bytewise_reference() {
      fn reference(bytes: &[u8]) -> u32 {
         let mut crc = !0u32;
         for &byte in bytes {
            crc = (crc >> 8) ^ CRC32_TABLES[0][((crc ^ u32::from(byte)) & 0xff) as usize];
         }
         !crc
      }
      assert_eq!(checksum(b"123456789"), 0xcbf4_3926);
      let data: Vec<u8> = (0..=255u8).cycle().take(1000).collect();
      for length in 0..data.len() {
         assert_eq!(
            checksum(&data[..length]),
            reference(&data[..length]),
            "length {length}"
         );
      }
   }
}
