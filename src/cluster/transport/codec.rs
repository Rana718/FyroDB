use std::io::{self, Read, Write};

const MAGIC: [u8; 4] = *b"FYRC";
const VERSION: u8 = 1;
const HEADER_LEN: usize = 40;
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
        if frame.payload.len() > self.max_payload {
            return Err(ProtocolError::PayloadTooLarge {
                length: frame.payload.len(),
                maximum: self.max_payload,
            });
        }
        let mut header = [0u8; HEADER_LEN];
        header[0..4].copy_from_slice(&MAGIC);
        header[4] = VERSION;
        header[5] = frame.message_type as u8;
        header[6..8].copy_from_slice(&frame.flags.to_be_bytes());
        header[8..16].copy_from_slice(&frame.request_id.to_be_bytes());
        header[16..24].copy_from_slice(&frame.source_id.to_be_bytes());
        header[24..32].copy_from_slice(&frame.target_id.to_be_bytes());
        header[32..40].copy_from_slice(&frame.epoch.to_be_bytes());
        writer.write_all(&header)?;
        writer.write_all(&(frame.payload.len() as u32).to_be_bytes())?;
        writer.write_all(&checksum(&frame.payload).to_be_bytes())?;
        writer.write_all(&frame.payload)?;
        Ok(())
    }

    pub fn read_frame(&self, reader: &mut impl Read) -> Result<Frame, ProtocolError> {
        let mut header = [0u8; HEADER_LEN];
        reader.read_exact(&mut header)?;
        if header[0..4] != MAGIC {
            return Err(ProtocolError::BadMagic);
        }
        if header[4] != VERSION {
            return Err(ProtocolError::UnsupportedVersion(header[4]));
        }
        let message_type = MessageType::try_from(header[5])?;
        let mut trailer = [0u8; 8];
        reader.read_exact(&mut trailer)?;
        let length = u32::from_be_bytes(trailer[0..4].try_into().unwrap()) as usize;
        if length > self.max_payload {
            return Err(ProtocolError::PayloadTooLarge {
                length,
                maximum: self.max_payload,
            });
        }
        let expected_checksum = u32::from_be_bytes(trailer[4..8].try_into().unwrap());
        let mut payload = vec![0u8; length];
        reader.read_exact(&mut payload)?;
        if checksum(&payload) != expected_checksum {
            return Err(ProtocolError::BadChecksum);
        }
        Ok(Frame {
            message_type,
            flags: u16::from_be_bytes(header[6..8].try_into().unwrap()),
            request_id: u64::from_be_bytes(header[8..16].try_into().unwrap()),
            source_id: u64::from_be_bytes(header[16..24].try_into().unwrap()),
            target_id: u64::from_be_bytes(header[24..32].try_into().unwrap()),
            epoch: u64::from_be_bytes(header[32..40].try_into().unwrap()),
            payload,
        })
    }
}

fn checksum(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in bytes {
        crc ^= byte as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
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
}
