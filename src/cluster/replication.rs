use super::Slot;
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationKind {
    Set = 1,
    Delete = 2,
    Expire = 3,
    Replace = 4,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationRecord {
    pub offset: u64,
    pub slot: Slot,
    pub kind: MutationKind,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub expire_at_ms: Option<u64>,
}

impl MutationRecord {
    pub fn replace(slot: Slot, key: Vec<u8>, value: Vec<u8>, expire_at_ms: Option<u64>) -> Self {
        Self {
            offset: 0,
            slot,
            kind: MutationKind::Replace,
            key,
            value,
            expire_at_ms,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationCodecError {
    Truncated,
    Invalid,
    TooLarge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogError {
    OffsetGap,
    OffsetRegression,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyError {
    OffsetGap,
    InvalidKey,
    InvalidValue,
    WrongSlot,
}

pub struct ReplicaApplier {
    applied_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationMessage {
    Begin {
        epoch: u64,
        from_offset: u64,
        identity: [u8; 16],
    },
    Entry(MutationRecord),
    Ack {
        applied_offset: u64,
    },
    Snapshot {
        epoch: u64,
        offset: u64,
        payload: Vec<u8>,
    },
    Finish {
        offset: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamError {
    NotStarted,
    WrongEpoch,
    OffsetGap,
    InvalidAck,
    AlreadyFinished,
}

pub struct ReplicationStream {
    epoch: u64,
    next_offset: u64,
    started: bool,
    finished: bool,
}

impl ReplicationStream {
    pub fn new() -> Self {
        Self {
            epoch: 0,
            next_offset: 1,
            started: false,
            finished: false,
        }
    }
    pub fn begin(&mut self, epoch: u64, from_offset: u64) -> Result<(), StreamError> {
        if self.started && !self.finished {
            return Err(StreamError::AlreadyFinished);
        }
        self.epoch = epoch;
        self.next_offset = from_offset.max(1);
        self.started = true;
        self.finished = false;
        Ok(())
    }
    pub fn entry(&mut self, record: &MutationRecord) -> Result<(), StreamError> {
        if !self.started || self.finished {
            return Err(StreamError::NotStarted);
        }
        if record.offset != self.next_offset {
            return Err(StreamError::OffsetGap);
        }
        self.next_offset = self.next_offset.saturating_add(1);
        Ok(())
    }
    pub fn ack(&self, offset: u64) -> Result<(), StreamError> {
        if !self.started || offset >= self.next_offset {
            return Err(StreamError::InvalidAck);
        }
        Ok(())
    }
    pub fn finish(&mut self, offset: u64) -> Result<(), StreamError> {
        if !self.started || self.finished || offset != self.next_offset.saturating_sub(1) {
            return Err(StreamError::InvalidAck);
        }
        self.finished = true;
        Ok(())
    }
    pub fn apply(&mut self, message: &ReplicationMessage) -> Result<(), StreamError> {
        match message {
            ReplicationMessage::Begin {
                epoch, from_offset, ..
            } => self.begin(*epoch, *from_offset),
            ReplicationMessage::Entry(record) => self.entry(record),
            ReplicationMessage::Ack { applied_offset } => self.ack(*applied_offset),
            ReplicationMessage::Finish { offset } => self.finish(*offset),
            ReplicationMessage::Snapshot { epoch, offset, .. } => {
                if self.started && *epoch != self.epoch {
                    return Err(StreamError::WrongEpoch);
                }
                self.epoch = *epoch;
                self.next_offset = offset.saturating_add(1);
                self.started = true;
                self.finished = false;
                Ok(())
            }
        }
    }
}

impl Default for ReplicationStream {
    fn default() -> Self {
        Self::new()
    }
}

const MAX_FIELD: usize = 16 * 1024 * 1024;
/// Hard upper bound for retained encoded mutation payloads per node.
/// Caps RSS consumption when values are large, independent of Store capacity.
const MAX_LOG_BYTES: usize = 64 * 1024 * 1024;

pub fn encode_replication_message(
    message: &ReplicationMessage,
) -> Result<Vec<u8>, ReplicationCodecError> {
    let mut out = Vec::new();
    match message {
        ReplicationMessage::Begin {
            epoch,
            from_offset,
            identity,
        } => {
            out.push(1);
            out.extend_from_slice(&epoch.to_be_bytes());
            out.extend_from_slice(&from_offset.to_be_bytes());
            out.extend_from_slice(identity);
        }
        ReplicationMessage::Entry(record) => {
            out.push(2);
            out.extend_from_slice(&encode_mutation(record)?);
        }
        ReplicationMessage::Ack { applied_offset } => {
            out.push(3);
            out.extend_from_slice(&applied_offset.to_be_bytes());
        }
        ReplicationMessage::Snapshot {
            epoch,
            offset,
            payload,
        } => {
            if payload.len() > MAX_FIELD {
                return Err(ReplicationCodecError::TooLarge);
            }
            out.push(4);
            out.extend_from_slice(&epoch.to_be_bytes());
            out.extend_from_slice(&offset.to_be_bytes());
            out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            out.extend_from_slice(payload);
        }
        ReplicationMessage::Finish { offset } => {
            out.push(5);
            out.extend_from_slice(&offset.to_be_bytes());
        }
    }
    Ok(out)
}

pub fn decode_replication_message(
    input: &[u8],
) -> Result<ReplicationMessage, ReplicationCodecError> {
    if input.is_empty() {
        return Err(ReplicationCodecError::Truncated);
    }
    match input[0] {
        1 if input.len() == 33 => Ok(ReplicationMessage::Begin {
            epoch: u64::from_be_bytes(input[1..9].try_into().unwrap()),
            from_offset: u64::from_be_bytes(input[9..17].try_into().unwrap()),
            identity: input[17..33].try_into().unwrap(),
        }),
        2 => Ok(ReplicationMessage::Entry(decode_mutation(&input[1..])?)),
        3 if input.len() == 9 => Ok(ReplicationMessage::Ack {
            applied_offset: u64::from_be_bytes(input[1..9].try_into().unwrap()),
        }),
        4 if input.len() >= 21 => {
            let length = u32::from_be_bytes(input[17..21].try_into().unwrap()) as usize;
            if length > MAX_FIELD || input.len() != 21 + length {
                return Err(ReplicationCodecError::Invalid);
            }
            Ok(ReplicationMessage::Snapshot {
                epoch: u64::from_be_bytes(input[1..9].try_into().unwrap()),
                offset: u64::from_be_bytes(input[9..17].try_into().unwrap()),
                payload: input[21..].to_vec(),
            })
        }
        5 if input.len() == 9 => Ok(ReplicationMessage::Finish {
            offset: u64::from_be_bytes(input[1..9].try_into().unwrap()),
        }),
        _ => Err(ReplicationCodecError::Invalid),
    }
}

impl ReplicaApplier {
    pub fn new(applied_offset: u64) -> Self {
        Self { applied_offset }
    }
    pub fn applied_offset(&self) -> u64 {
        self.applied_offset
    }
    pub fn reset(&mut self, offset: u64) {
        self.applied_offset = offset;
    }
    pub fn apply<F>(&mut self, record: &MutationRecord, mut apply: F) -> Result<bool, ApplyError>
    where
        F: FnMut(&MutationRecord),
    {
        self.try_apply(record, |record| {
            apply(record);
            Ok(())
        })
    }

    pub fn try_apply<F>(
        &mut self,
        record: &MutationRecord,
        mut apply: F,
    ) -> Result<bool, ApplyError>
    where
        F: FnMut(&MutationRecord) -> Result<(), ApplyError>,
    {
        if record.offset <= self.applied_offset {
            return Ok(false);
        }
        if record.offset != self.applied_offset.saturating_add(1) {
            return Err(ApplyError::OffsetGap);
        }
        apply(record)?;
        self.applied_offset = record.offset;
        Ok(true)
    }
}

pub struct MutationLog {
    next_offset: u64,
    capacity: usize,
    bytes: usize,
    records: VecDeque<MutationRecord>,
}

#[derive(Clone)]
pub struct ReplicationCoordinator {
    log: Arc<Mutex<MutationLog>>,
    next_offset: Arc<AtomicU64>,
    appended: Arc<AtomicU64>,
    journal: Arc<Mutex<Option<ReplicationJournal>>>,
}

struct ReplicationJournal {
    file: std::fs::File,
    identity: [u8; 16],
}

impl ReplicationCoordinator {
    pub fn new(capacity: usize) -> Self {
        Self {
            log: Arc::new(Mutex::new(MutationLog::new(capacity))),
            next_offset: Arc::new(AtomicU64::new(1)),
            appended: Arc::new(AtomicU64::new(0)),
            journal: Arc::new(Mutex::new(None)),
        }
    }
    pub fn append(&self, record: MutationRecord) -> Result<u64, LogError> {
        let offset = self.next_offset.fetch_add(1, Ordering::Relaxed);
        let mut record = record;
        record.offset = offset;

        let journal_payload = if self.journal.lock().unwrap().is_some() {
            encode_mutation(&record).ok()
        } else {
            None
        };

        let result = self
            .log
            .lock()
            .expect("replication log poisoned")
            .append(record);

        if result.is_ok() {
            self.appended.fetch_add(1, Ordering::Relaxed);
            if let Some(payload) = journal_payload
                && let Some(journal) = self.journal.lock().unwrap().as_mut()
            {
                let _ = journal
                    .file
                    .write_all(&(payload.len() as u32).to_le_bytes());
                let _ = journal.file.write_all(&payload);
            }
        }
        result
    }
    pub fn replay_from(&self, offset: u64) -> Result<Vec<MutationRecord>, LogError> {
        self.log
            .lock()
            .expect("replication log poisoned")
            .replay_from(offset)
    }
    pub fn record_at(&self, offset: u64) -> Result<Option<MutationRecord>, LogError> {
        self.log
            .lock()
            .expect("replication log poisoned")
            .record_at(offset)
    }
    /// Returns the first retained offset, or the next offset when the log is empty.
    pub fn first_retained_offset(&self) -> u64 {
        self.log
            .lock()
            .expect("replication log poisoned")
            .records
            .front()
            .map_or_else(|| self.next_offset(), |record| record.offset)
    }
    pub fn next_offset(&self) -> u64 {
        self.next_offset.load(Ordering::Relaxed)
    }
    pub fn len(&self) -> usize {
        self.log.lock().expect("replication log poisoned").len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn appended_count(&self) -> u64 {
        self.appended.load(Ordering::Relaxed)
    }
    pub fn retained_capacity(&self) -> usize {
        self.log.lock().expect("replication log poisoned").capacity
    }
    pub fn retained_bytes(&self) -> usize {
        self.log.lock().expect("replication log poisoned").bytes()
    }
    pub fn retained_byte_limit(&self) -> usize {
        MAX_LOG_BYTES
    }

    pub fn flush_journal(&self) {
        use std::os::unix::io::AsRawFd;
        if let Some(journal) = self.journal.lock().unwrap().as_mut() {
            let _ = journal.file.flush();
            unsafe { libc::fsync(journal.file.as_raw_fd()) };
        }
    }

    pub fn identity(&self) -> Option<[u8; 16]> {
        self.journal
            .lock()
            .unwrap()
            .as_ref()
            .map(|journal| journal.identity)
    }

    pub fn open_journal(&self, path: &str, identity: [u8; 16]) -> std::io::Result<()> {
        const MAGIC: &[u8; 4] = b"FLRJ";
        let mut restored = Vec::new();
        if std::path::Path::new(path).exists() {
            let mut file = std::fs::File::open(path)?;
            let mut header = [0u8; 21];
            file.read_exact(&mut header)?;
            if &header[..4] != MAGIC || header[4] != 1 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "replication journal identity mismatch",
                ));
            }
            let existing: [u8; 16] = header[5..21].try_into().unwrap();
            let existing_identity = if identity == [0; 16] {
                existing
            } else if existing != identity {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "replication journal identity mismatch",
                ));
            } else {
                identity
            };
            let _ = existing_identity;
            loop {
                let mut length = [0u8; 4];
                match file.read_exact(&mut length) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(error) => return Err(error),
                }
                let length = u32::from_le_bytes(length) as usize;
                if length > MAX_FIELD {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "oversized replication journal record",
                    ));
                }
                let mut payload = vec![0u8; length];
                file.read_exact(&mut payload)?;
                restored.push(decode_mutation(&payload).map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "invalid replication journal record",
                    )
                })?);
            }
        }
        if !restored.is_empty() {
            let mut log = self.log.lock().unwrap();
            for record in restored {
                let _ = log.append(record);
            }
            self.next_offset.store(log.next_offset(), Ordering::Relaxed);
        }
        let exists = std::path::Path::new(path).exists();
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        if !exists {
            file.write_all(MAGIC)?;
            file.write_all(&[1])?;
            file.write_all(&identity)?;
            file.flush()?;
        }
        *self.journal.lock().unwrap() = Some(ReplicationJournal { file, identity });
        Ok(())
    }
}

impl MutationLog {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            next_offset: 1,
            capacity,
            bytes: 0,
            // Do not reserve the configured maximum up front. A production
            // capacity of 100k records would otherwise reserve several MiB on
            // every cluster node before the first write. VecDeque grows only
            // with retained replication data and remains bounded below.
            records: VecDeque::new(),
        }
    }
    pub fn append(&mut self, mut record: MutationRecord) -> Result<u64, LogError> {
        if record.offset != 0 && record.offset < self.next_offset {
            return Err(LogError::OffsetRegression);
        }
        record.offset = self.next_offset;
        self.next_offset = self.next_offset.saturating_add(1);
        self.bytes = self.bytes.saturating_add(record_memory_bytes(&record));
        self.records.push_back(record);
        while self.records.len() > self.capacity || self.bytes > MAX_LOG_BYTES {
            if let Some(old) = self.records.pop_front() {
                self.bytes = self.bytes.saturating_sub(record_memory_bytes(&old));
            } else {
                break;
            }
        }
        Ok(self.next_offset - 1)
    }
    pub fn replay_from(&self, offset: u64) -> Result<Vec<MutationRecord>, LogError> {
        let first = self
            .records
            .front()
            .map_or(self.next_offset, |record| record.offset);
        if offset != 0 && offset < first {
            return Err(LogError::OffsetGap);
        }
        Ok(self
            .records
            .iter()
            .filter(|record| record.offset >= offset.max(1))
            .cloned()
            .collect())
    }
    pub fn record_at(&self, offset: u64) -> Result<Option<MutationRecord>, LogError> {
        let first = self
            .records
            .front()
            .map_or(self.next_offset, |record| record.offset);
        if offset != 0 && offset < first {
            return Err(LogError::OffsetGap);
        }
        Ok(self
            .records
            .iter()
            .find(|record| record.offset == offset.max(1))
            .cloned())
    }
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }
    pub fn len(&self) -> usize {
        self.records.len()
    }
    pub fn bytes(&self) -> usize {
        self.bytes
    }
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

#[inline]
fn record_memory_bytes(record: &MutationRecord) -> usize {
    record
        .key
        .len()
        .saturating_add(record.value.len())
        .saturating_add(std::mem::size_of::<MutationRecord>())
}

pub fn encode_mutation(record: &MutationRecord) -> Result<Vec<u8>, ReplicationCodecError> {
    if record.key.len() > MAX_FIELD || record.value.len() > MAX_FIELD {
        return Err(ReplicationCodecError::TooLarge);
    }
    let mut out = Vec::with_capacity(32 + record.key.len() + record.value.len());
    out.extend_from_slice(&record.offset.to_be_bytes());
    out.extend_from_slice(&record.slot.value().to_be_bytes());
    out.push(record.kind as u8);
    out.extend_from_slice(&(record.key.len() as u32).to_be_bytes());
    out.extend_from_slice(&(record.value.len() as u32).to_be_bytes());
    out.extend_from_slice(&record.expire_at_ms.unwrap_or(0).to_be_bytes());
    out.extend_from_slice(&record.key);
    out.extend_from_slice(&record.value);
    Ok(out)
}

pub fn decode_mutation(mut input: &[u8]) -> Result<MutationRecord, ReplicationCodecError> {
    let offset = u64::from_be_bytes(take(&mut input, 8)?.try_into().unwrap());
    let slot = Slot::new(u16::from_be_bytes(take(&mut input, 2)?.try_into().unwrap()))
        .ok_or(ReplicationCodecError::Invalid)?;
    let kind = match take(&mut input, 1)?[0] {
        1 => MutationKind::Set,
        2 => MutationKind::Delete,
        3 => MutationKind::Expire,
        4 => MutationKind::Replace,
        _ => return Err(ReplicationCodecError::Invalid),
    };
    let key_len = u32::from_be_bytes(take(&mut input, 4)?.try_into().unwrap()) as usize;
    let value_len = u32::from_be_bytes(take(&mut input, 4)?.try_into().unwrap()) as usize;
    let expire = u64::from_be_bytes(take(&mut input, 8)?.try_into().unwrap());
    if key_len > MAX_FIELD || value_len > MAX_FIELD {
        return Err(ReplicationCodecError::TooLarge);
    }
    let key = take(&mut input, key_len)?.to_vec();
    let value = take(&mut input, value_len)?.to_vec();
    if !input.is_empty()
        || key.is_empty()
        || (matches!(kind, MutationKind::Delete | MutationKind::Expire) && !value.is_empty())
    {
        return Err(ReplicationCodecError::Invalid);
    }
    Ok(MutationRecord {
        offset,
        slot,
        kind,
        key,
        value,
        expire_at_ms: (expire != 0).then_some(expire),
    })
}

fn take<'a>(input: &mut &'a [u8], size: usize) -> Result<&'a [u8], ReplicationCodecError> {
    if input.len() < size {
        return Err(ReplicationCodecError::Truncated);
    }
    let (part, rest) = input.split_at(size);
    *input = rest;
    Ok(part)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mutation_round_trip_and_validation() {
        let record = MutationRecord {
            offset: 9,
            slot: Slot(42),
            kind: MutationKind::Set,
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            expire_at_ms: Some(123),
        };
        let bytes = encode_mutation(&record).unwrap();
        assert_eq!(decode_mutation(&bytes).unwrap(), record);
        assert!(matches!(
            decode_mutation(&bytes[..bytes.len() - 1]),
            Err(ReplicationCodecError::Truncated)
        ));
    }

    #[test]
    fn log_assigns_offsets_and_reports_retention_gaps() {
        let mut log = MutationLog::new(2);
        let record = || MutationRecord {
            offset: 0,
            slot: Slot(1),
            kind: MutationKind::Delete,
            key: b"k".to_vec(),
            value: Vec::new(),
            expire_at_ms: None,
        };
        assert_eq!(log.append(record()).unwrap(), 1);
        assert_eq!(log.append(record()).unwrap(), 2);
        assert_eq!(log.append(record()).unwrap(), 3);
        assert_eq!(log.replay_from(2).unwrap().len(), 2);
        assert_eq!(log.replay_from(1), Err(LogError::OffsetGap));
    }

    #[test]
    fn log_has_a_byte_bound_for_large_values() {
        let mut log = MutationLog::new(100);
        let record = MutationRecord {
            offset: 0,
            slot: Slot(1),
            kind: MutationKind::Set,
            key: b"k".to_vec(),
            value: vec![b'x'; 16 * 1024 * 1024],
            expire_at_ms: None,
        };
        for _ in 0..8 {
            log.append(record.clone()).unwrap();
        }
        assert!(log.len() < 8);
        assert!(log.replay_from(1).is_err());
    }

    #[test]
    fn replica_apply_is_idempotent_and_rejects_gaps() {
        let mut applier = ReplicaApplier::new(0);
        let record = |offset| MutationRecord {
            offset,
            slot: Slot(1),
            kind: MutationKind::Delete,
            key: b"k".to_vec(),
            value: Vec::new(),
            expire_at_ms: None,
        };
        let mut applied = 0;
        assert!(applier.apply(&record(1), |_| applied += 1).unwrap());
        assert!(!applier.apply(&record(1), |_| applied += 1).unwrap());
        assert_eq!(
            applier.apply(&record(3), |_| applied += 1),
            Err(ApplyError::OffsetGap)
        );
        assert_eq!(applied, 1);
    }

    #[test]
    fn stream_enforces_order_and_offsets() {
        let mut stream = ReplicationStream::new();
        assert_eq!(
            stream.apply(&ReplicationMessage::Entry(MutationRecord {
                offset: 1,
                slot: Slot(1),
                kind: MutationKind::Delete,
                key: b"k".to_vec(),
                value: Vec::new(),
                expire_at_ms: None
            })),
            Err(StreamError::NotStarted)
        );
        stream.begin(3, 1).unwrap();
        let record = MutationRecord {
            offset: 1,
            slot: Slot(1),
            kind: MutationKind::Delete,
            key: b"k".to_vec(),
            value: Vec::new(),
            expire_at_ms: None,
        };
        stream.entry(&record).unwrap();
        assert!(stream.ack(1).is_ok());
        stream.finish(1).unwrap();
        assert_eq!(stream.finish(1), Err(StreamError::InvalidAck));
    }

    #[test]
    fn coordinator_shares_bounded_primary_log() {
        let coordinator = ReplicationCoordinator::new(2);
        let record = MutationRecord {
            offset: 0,
            slot: Slot(1),
            kind: MutationKind::Delete,
            key: b"k".to_vec(),
            value: Vec::new(),
            expire_at_ms: None,
        };
        assert_eq!(coordinator.append(record.clone()).unwrap(), 1);
        assert_eq!(coordinator.append(record).unwrap(), 2);
        assert_eq!(coordinator.len(), 2);
        assert_eq!(coordinator.replay_from(1).unwrap().len(), 2);
        assert_eq!(coordinator.appended_count(), 2);
        assert_eq!(coordinator.retained_capacity(), 2);
    }
}
