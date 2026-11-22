use super::Slot;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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
   /// Shared with the mutation log so fan-out to a replica does not clone the
   /// key/value buffers of a retained entry.
   Entry(Arc<MutationRecord>),
   /// Consecutive log entries in one frame, acknowledged once. Sending one
   /// request and one ack per mutation cost the primary ~2 µs of CPU per
   /// write and capped SET throughput with a replica at about a third of the
   /// replica-less rate.
   Entries(Vec<Arc<MutationRecord>>),
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
         ReplicationMessage::Entries(records) => {
            records.iter().try_for_each(|record| self.entry(record))
         }
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

/// Set by a replica on its reply to `ReplicationBegin` when it accepts
/// `ReplicationMessage::Entries` batches. Older nodes ignore unknown flag bits
/// and older replicas never set it, so a new primary falls back to one entry
/// per frame for them: nodes can be upgraded in any order.
pub const FLAG_REPLICATION_BATCHES: u16 = 1 << 8;
/// Hard upper bound for retained encoded mutation payloads per node.
/// Caps RSS consumption when values are large, independent of Store capacity.
const MAX_LOG_BYTES: usize = 64 * 1024 * 1024;

pub fn encode_replication_message(
   message: &ReplicationMessage,
) -> Result<Vec<u8>, ReplicationCodecError> {
   let mut out = Vec::with_capacity(48);
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
         encode_mutation_to_buf(record, &mut out)?;
      }
      ReplicationMessage::Entries(records) => {
         out.reserve(
            5 + records
               .iter()
               .map(|r| 4 + MUTATION_HEADER_LEN + r.key.len() + r.value.len())
               .sum::<usize>(),
         );
         out.push(6);
         out.extend_from_slice(&(records.len() as u32).to_be_bytes());
         for record in records {
            if record.key.len() > MAX_FIELD || record.value.len() > MAX_FIELD {
               return Err(ReplicationCodecError::TooLarge);
            }
            let record_len = MUTATION_HEADER_LEN + record.key.len() + record.value.len();
            out.extend_from_slice(&(record_len as u32).to_be_bytes());
            encode_mutation_to_buf(record, &mut out)?;
         }
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
      2 => Ok(ReplicationMessage::Entry(Arc::new(decode_mutation(
         &input[1..],
      )?))),
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
      6 => Ok(ReplicationMessage::Entries(
         decode_mutation_batch(&input[1..])?
            .into_iter()
            .map(Arc::new)
            .collect(),
      )),
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

   pub fn try_apply<F>(&mut self, record: &MutationRecord, mut apply: F) -> Result<bool, ApplyError>
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
   /// Shared records avoiding deep buffer copies on replication fan-out.
   records: VecDeque<Arc<MutationRecord>>,
}

/// Lightweight spinlock with yield fallback for uncontended per-thread logging.
pub struct SpinMutex<T> {
   state: AtomicU64,
   data: std::cell::UnsafeCell<T>,
}

unsafe impl<T: Send> Sync for SpinMutex<T> {}
unsafe impl<T: Send> Send for SpinMutex<T> {}

impl<T> SpinMutex<T> {
   pub fn new(data: T) -> Self {
      SpinMutex {
         state: AtomicU64::new(0),
         data: std::cell::UnsafeCell::new(data),
      }
   }
   #[inline]
   pub fn lock(&self) -> SpinGuard<'_, T> {
      let mut backoff = 1u32;
      loop {
         if self
            .state
            .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
         {
            return SpinGuard { lock: self };
         }
         while self.state.load(Ordering::Relaxed) != 0 {
            for _ in 0..backoff {
               std::hint::spin_loop();
            }
            if backoff < 64 {
               backoff <<= 1;
            } else {
               std::thread::yield_now();
               backoff = 1;
            }
         }
      }
   }
}

pub struct SpinGuard<'a, T: 'a> {
   lock: &'a SpinMutex<T>,
}

impl<T> std::ops::Deref for SpinGuard<'_, T> {
   type Target = T;
   #[inline]
   fn deref(&self) -> &T {
      unsafe { &*self.lock.data.get() }
   }
}

impl<T> std::ops::DerefMut for SpinGuard<'_, T> {
   #[inline]
   fn deref_mut(&mut self) -> &mut T {
      unsafe { &mut *self.lock.data.get() }
   }
}

impl<T> Drop for SpinGuard<'_, T> {
   #[inline]
   fn drop(&mut self) {
      self.lock.state.store(0, Ordering::Release);
   }
}

#[derive(Clone)]
pub struct ReplicationCoordinator {
   state: Arc<Mutex<CoordinatorState>>,
   /// Set when the journal buffer holds entries; lets the per-loop flush
   /// skip the lock when nothing was appended.
   journal_dirty: Arc<std::sync::atomic::AtomicBool>,
}

struct CoordinatorState {
   log: MutationLog,
   journal: Option<ReplicationJournal>,
}

struct ReplicationJournal {
   file: std::fs::File,
   identity: [u8; 16],
   /// Encoded entries not yet written to `file`. Writing each mutation with
   /// its own syscall, under the log lock every writer shares, cost a write
   /// path with a replica ~4x the CPU of one without. Entries are written in
   /// bulk before any of them is handed to a replica (see `record_at`), so a
   /// replica never holds an entry the journal lacks.
   pending: Vec<u8>,
   /// Highest offset whose entry is in `pending`.
   pending_through: u64,
}

/// Write the buffer out once it holds this much, regardless of readers.
const JOURNAL_BUFFER_BYTES: usize = 1 << 20;

impl ReplicationJournal {
   fn new(file: std::fs::File, identity: [u8; 16]) -> Self {
      Self {
         file,
         identity,
         pending: Vec::new(),
         pending_through: 0,
      }
   }

   fn write_pending(&mut self) {
      if self.pending.is_empty() {
         return;
      }
      let _ = self.file.write_all(&self.pending);
      self.pending.clear();
      if self.pending.capacity() > 4 * JOURNAL_BUFFER_BYTES {
         self.pending.shrink_to(JOURNAL_BUFFER_BYTES);
      }
   }
}

impl Drop for ReplicationJournal {
   fn drop(&mut self) {
      self.write_pending();
   }
}

/// Wire length of [`encode_mutation`]'s fixed prefix.
const MUTATION_HEADER_LEN: usize = 27;

/// Appends journal record as `u32 length || mutation || key || value` to the
/// journal's buffer.
fn write_journal_entry(journal: &mut ReplicationJournal, record: &MutationRecord) {
   if record.key.len() > MAX_FIELD || record.value.len() > MAX_FIELD {
      return;
   }
   let encoded_len = MUTATION_HEADER_LEN + record.key.len() + record.value.len();
   let out = &mut journal.pending;
   out.reserve(4 + encoded_len);
   out.extend_from_slice(&(encoded_len as u32).to_le_bytes());
   out.extend_from_slice(&record.offset.to_be_bytes());
   out.extend_from_slice(&record.slot.value().to_be_bytes());
   out.push(record.kind as u8);
   out.extend_from_slice(&(record.key.len() as u32).to_be_bytes());
   out.extend_from_slice(&(record.value.len() as u32).to_be_bytes());
   out.extend_from_slice(&record.expire_at_ms.unwrap_or(0).to_be_bytes());
   out.extend_from_slice(&record.key);
   out.extend_from_slice(&record.value);
   journal.pending_through = record.offset;
   if journal.pending.len() >= JOURNAL_BUFFER_BYTES {
      journal.write_pending();
   }
}

/// The replication log held for appending; offsets are assigned in append order.
pub struct AppendGuard<'a> {
   state: parking_lot::MutexGuard<'a, CoordinatorState>,
   journal_dirty: &'a std::sync::atomic::AtomicBool,
}

impl AppendGuard<'_> {
   pub fn append(&mut self, mut record: MutationRecord) -> Result<u64, LogError> {
      record.offset = self.state.log.next_offset();
      if let Some(journal) = self.state.journal.as_mut() {
         write_journal_entry(journal, &record);
         self.journal_dirty.store(true, Ordering::Release);
      }
      self.state.log.append(record)
   }
}

impl ReplicationCoordinator {
   pub fn new(capacity: usize) -> Self {
      Self {
         state: Arc::new(Mutex::new(CoordinatorState {
            log: MutationLog::new(capacity),
            journal: None,
         })),
         journal_dirty: Arc::new(std::sync::atomic::AtomicBool::new(false)),
      }
   }

   pub fn append(&self, record: MutationRecord) -> Result<u64, LogError> {
      self.lock_append().append(record)
   }

   /// Hold the log while the caller decides what to append, so the offset is
   /// assigned at a point where the caller's view of the key cannot change.
   pub fn lock_append(&self) -> AppendGuard<'_> {
      AppendGuard {
         state: self.state.lock(),
         journal_dirty: &self.journal_dirty,
      }
   }

   pub fn replay_from(&self, offset: u64) -> Result<Vec<MutationRecord>, LogError> {
      let records: Vec<_> = {
         let mut state = self.state.lock();
         if let Some(journal) = state.journal.as_mut() {
            journal.write_pending();
         }
         let first = state.log.first_offset();
         if offset != 0 && offset < first {
            return Err(LogError::OffsetGap);
         }
         let skip = usize::try_from(offset.saturating_sub(first)).unwrap_or(usize::MAX);
         state.log.records.iter().skip(skip).cloned().collect()
      };
      Ok(records
         .into_iter()
         .map(|record| (*record).clone())
         .collect())
   }

   pub fn record_at(&self, offset: u64) -> Result<Option<Arc<MutationRecord>>, LogError> {
      let mut state = self.state.lock();
      // Entries leave for replicas only once they are in the journal file;
      // the first read past the written prefix writes the whole buffer, so a
      // batch costs one syscall instead of one per mutation.
      if let Some(journal) = state.journal.as_mut()
         && offset <= journal.pending_through
         && !journal.pending.is_empty()
      {
         journal.write_pending();
      }
      if offset != 0 && offset < state.log.first_offset() {
         return Err(LogError::OffsetGap);
      }
      state.log.record_at(offset)
   }
   pub fn first_retained_offset(&self) -> u64 {
      self.state.lock().log.first_offset()
   }
   pub fn next_offset(&self) -> u64 {
      self.state.lock().log.next_offset()
   }
   pub fn len(&self) -> usize {
      self.state.lock().log.len()
   }
   pub fn is_empty(&self) -> bool {
      self.state.lock().log.is_empty()
   }
   pub fn appended_count(&self) -> u64 {
      self.next_offset() - 1
   }
   pub fn retained_capacity(&self) -> usize {
      self.state.lock().log.capacity
   }
   pub fn retained_bytes(&self) -> usize {
      self.state.lock().log.bytes()
   }
   pub fn retained_byte_limit(&self) -> usize {
      MAX_LOG_BYTES
   }

   /// Writes buffered journal entries to the file (no fsync). Workers call
   /// this once per event-loop pass, right before the AOF writes its own
   /// buffers, so the journal never lags the AOF after a process crash and
   /// costs one syscall per batch of commands instead of one per mutation.
   pub fn write_journal_buffer(&self) {
      if self.journal_dirty.swap(false, Ordering::AcqRel)
         && let Some(journal) = self.state.lock().journal.as_mut()
      {
         journal.write_pending();
      }
   }

   pub fn flush_journal(&self) {
      use std::os::unix::io::AsRawFd;
      if let Some(journal) = self.state.lock().journal.as_mut() {
         journal.write_pending();
         let _ = journal.file.flush();
         unsafe { libc::fsync(journal.file.as_raw_fd()) };
      }
   }

   pub fn identity(&self) -> Option<[u8; 16]> {
      self
         .state
         .lock()
         .journal
         .as_ref()
         .map(|journal| journal.identity)
   }

   pub fn open_journal(&self, path: &str, mut identity: [u8; 16]) -> std::io::Result<()> {
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
         identity = if identity == [0; 16] {
            existing
         } else if existing != identity {
            return Err(std::io::Error::new(
               std::io::ErrorKind::InvalidData,
               "replication journal identity mismatch",
            ));
         } else {
            identity
         };
         loop {
            let mut length = [0u8; 4];
            match file.read_exact(&mut length) {
               Ok(()) => {}
               Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
               Err(error) => return Err(error),
            }
            let length = u32::from_le_bytes(length) as usize;
            if length > MAX_FIELD.saturating_mul(2).saturating_add(64) {
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
      restored.sort_by_key(|record| record.offset);
      let mut state = self.state.lock();
      for mut record in restored {
         record.offset = 0;
         state
            .log
            .append(record)
            .expect("restore assigns fresh offsets");
      }
      state.journal = Some(ReplicationJournal::new(file, identity));
      Ok(())
   }
}

impl MutationLog {
   fn first_offset(&self) -> u64 {
      self
         .records
         .front()
         .map_or(self.next_offset, |record| record.offset)
   }
   pub fn new(capacity: usize) -> Self {
      let capacity = capacity.max(1);
      Self {
         next_offset: 1,
         capacity,
         bytes: 0,
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
      self.records.push_back(Arc::new(record));
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
         .map(|record| MutationRecord::clone(record))
         .collect())
   }
   pub fn record_at(&self, offset: u64) -> Result<Option<Arc<MutationRecord>>, LogError> {
      let Some(front) = self.records.front() else {
         return Ok(None);
      };
      let first = front.offset;
      if offset != 0 && offset < first {
         return Err(LogError::OffsetGap);
      }
      let offset = offset.max(1);
      if offset < first {
         return Ok(None);
      }
      // O(1) arithmetic offset lookup sharing retained record Arc.
      Ok(self.records.get((offset - first) as usize).map(Arc::clone))
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
      .capacity()
      .saturating_add(record.value.capacity())
      .saturating_add(std::mem::size_of::<MutationRecord>())
}

#[inline]
pub fn encode_mutation_to_buf(
   record: &MutationRecord,
   out: &mut Vec<u8>,
) -> Result<(), ReplicationCodecError> {
   if record.key.len() > MAX_FIELD || record.value.len() > MAX_FIELD {
      return Err(ReplicationCodecError::TooLarge);
   }
   out.reserve(MUTATION_HEADER_LEN + record.key.len() + record.value.len());
   out.extend_from_slice(&record.offset.to_be_bytes());
   out.extend_from_slice(&record.slot.value().to_be_bytes());
   out.push(record.kind as u8);
   out.extend_from_slice(&(record.key.len() as u32).to_be_bytes());
   out.extend_from_slice(&(record.value.len() as u32).to_be_bytes());
   out.extend_from_slice(&record.expire_at_ms.unwrap_or(0).to_be_bytes());
   out.extend_from_slice(&record.key);
   out.extend_from_slice(&record.value);
   Ok(())
}

/// Encoded size of one mutation, as carried in replication frames.
pub fn mutation_wire_len(record: &MutationRecord) -> usize {
   MUTATION_HEADER_LEN + record.key.len() + record.value.len()
}

pub fn encode_mutation(record: &MutationRecord) -> Result<Vec<u8>, ReplicationCodecError> {
   if record.key.len() > MAX_FIELD || record.value.len() > MAX_FIELD {
      return Err(ReplicationCodecError::TooLarge);
   }
   let mut out = Vec::with_capacity(MUTATION_HEADER_LEN + record.key.len() + record.value.len());
   encode_mutation_to_buf(record, &mut out)?;
   Ok(out)
}

/// Migration chunks carry a count followed by length-prefixed mutations, so a
/// whole batch costs one network round trip instead of one per key.
pub fn encode_mutation_batch(records: &[MutationRecord]) -> Result<Vec<u8>, ReplicationCodecError> {
   let total_len = records.iter().fold(4usize, |acc, r| {
      acc + 4 + MUTATION_HEADER_LEN + r.key.len() + r.value.len()
   });
   let mut out = Vec::with_capacity(total_len);
   out.extend_from_slice(&(records.len() as u32).to_be_bytes());
   for record in records {
      let record_len = MUTATION_HEADER_LEN + record.key.len() + record.value.len();
      out.extend_from_slice(&(record_len as u32).to_be_bytes());
      encode_mutation_to_buf(record, &mut out)?;
   }
   Ok(out)
}

pub fn decode_mutation_batch(input: &[u8]) -> Result<Vec<MutationRecord>, ReplicationCodecError> {
   if input.len() < 4 {
      return Err(ReplicationCodecError::Truncated);
   }
   let count = u32::from_be_bytes(input[..4].try_into().unwrap()) as usize;
   // Guard against corrupt or malicious batch length amplification.
   if count > (1 << 20) || count > (input.len() - 4) / (4 + MUTATION_HEADER_LEN) {
      return Err(ReplicationCodecError::Invalid);
   }
   let mut rest = &input[4..];
   let mut records = Vec::with_capacity(count);
   for _ in 0..count {
      let length = u32::from_be_bytes(take(&mut rest, 4)?.try_into().unwrap()) as usize;
      if length > MAX_FIELD.saturating_mul(2).saturating_add(64) {
         return Err(ReplicationCodecError::TooLarge);
      }
      records.push(decode_mutation(take(&mut rest, length)?)?);
   }
   if !rest.is_empty() {
      return Err(ReplicationCodecError::Invalid);
   }
   Ok(records)
}

/// Snapshot chunks are built directly into their wire form to avoid copying
/// the payload twice (message body, then frame).
pub fn encode_snapshot(
   epoch: u64,
   offset: u64,
   payload: &[u8],
) -> Result<Vec<u8>, ReplicationCodecError> {
   if payload.len() > MAX_FIELD {
      return Err(ReplicationCodecError::TooLarge);
   }
   let mut out = Vec::with_capacity(21 + payload.len());
   out.push(4);
   out.extend_from_slice(&epoch.to_be_bytes());
   out.extend_from_slice(&offset.to_be_bytes());
   out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
   out.extend_from_slice(payload);
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
   fn interleaved_writers_return_the_requested_offsets() {
      let log = ReplicationCoordinator::new(10);
      let append = |value| {
         log.append(MutationRecord::replace(Slot(0), vec![1], vec![value], None))
            .unwrap()
      };
      assert_eq!(append(1), 1);
      std::thread::scope(|s| assert_eq!(s.spawn(|| append(2)).join().unwrap(), 2));
      assert_eq!(append(3), 3);
      for i in 1..=3 {
         let record = log.record_at(i).unwrap().unwrap();
         assert_eq!((record.offset, record.value[0]), (i, i as u8));
      }
   }

   #[test]
   fn journal_is_written_in_bulk_before_any_entry_reaches_a_replica() {
      let path = std::env::temp_dir().join(format!(
         "fyrodb-journal-bulk-{}-{}.log",
         std::process::id(),
         std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
      ));
      let path_str = path.to_str().unwrap().to_owned();
      let coordinator = ReplicationCoordinator::new(64);
      coordinator.open_journal(&path_str, [3; 16]).unwrap();
      let header = std::fs::metadata(&path).unwrap().len();
      for i in 0..10u8 {
         coordinator
            .append(MutationRecord::replace(
               Slot(1),
               vec![b'k', i],
               vec![i; 8],
               None,
            ))
            .unwrap();
      }
      assert_eq!(
         std::fs::metadata(&path).unwrap().len(),
         header,
         "appends are buffered, not one syscall each"
      );
      // The replication sender's first read writes every buffered entry.
      assert!(coordinator.record_at(1).unwrap().is_some());
      let entry = 4 + MUTATION_HEADER_LEN + 2 + 8;
      assert_eq!(
         std::fs::metadata(&path).unwrap().len(),
         header + 10 * entry as u64
      );
      drop(coordinator);
      let reopened = ReplicationCoordinator::new(64);
      reopened.open_journal(&path_str, [3; 16]).unwrap();
      assert_eq!(reopened.next_offset(), 11, "all ten entries replay");
      let _ = std::fs::remove_file(&path);
   }

   #[test]
   fn entry_batches_round_trip_in_order_and_reject_corruption() {
      let records: Vec<Arc<MutationRecord>> = (1..=300u64)
         .map(|offset| {
            let delete = offset % 3 == 0;
            Arc::new(MutationRecord {
               offset,
               slot: Slot((offset % 16384) as u16),
               kind: if delete {
                  MutationKind::Delete
               } else {
                  MutationKind::Set
               },
               key: format!("key:{offset}").into_bytes(),
               // Deletes carry no value on the wire.
               value: if delete {
                  Vec::new()
               } else {
                  vec![offset as u8; (offset % 7) as usize]
               },
               expire_at_ms: (!delete && offset % 5 == 0).then_some(offset * 1000),
            })
         })
         .collect();
      let bytes =
         encode_replication_message(&ReplicationMessage::Entries(records.clone())).unwrap();
      let Ok(ReplicationMessage::Entries(decoded)) = decode_replication_message(&bytes) else {
         panic!("batch did not decode");
      };
      assert_eq!(decoded, records);
      assert!(decode_replication_message(&bytes[..bytes.len() - 1]).is_err());
      // A count that claims more records than the frame can hold is refused
      // before anything is allocated.
      let mut lying = bytes.clone();
      lying[1..5].copy_from_slice(&u32::MAX.to_be_bytes());
      assert!(decode_replication_message(&lying).is_err());
   }

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
   fn mutation_batches_round_trip_and_reject_impossible_counts() {
      let records: Vec<MutationRecord> = (0..3)
         .map(|index| MutationRecord {
            offset: index + 1,
            slot: Slot(1),
            kind: MutationKind::Replace,
            key: format!("k{index}").into_bytes(),
            value: vec![index as u8; 64],
            expire_at_ms: None,
         })
         .collect();
      let batch = encode_mutation_batch(&records).unwrap();
      assert_eq!(decode_mutation_batch(&batch).unwrap(), records);

      // A count that cannot fit in the frame must be rejected before the
      // decoder reserves any memory for it.
      let mut bogus = 1_000_000u32.to_be_bytes().to_vec();
      assert!(matches!(
         decode_mutation_batch(&bogus),
         Err(ReplicationCodecError::Invalid)
      ));
      bogus.extend_from_slice(&batch[4..]);
      assert!(matches!(
         decode_mutation_batch(&bogus),
         Err(ReplicationCodecError::Invalid)
      ));
   }

   #[test]
   fn journal_entries_use_the_replication_wire_format_and_replay() {
      let path = std::env::temp_dir().join(format!(
         "fyrodb-journal-{}-{}.log",
         std::process::id(),
         std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
      ));
      let path_str = path.to_str().unwrap().to_owned();
      let record = MutationRecord {
         offset: 0,
         slot: Slot(42),
         kind: MutationKind::Replace,
         key: b"key".to_vec(),
         value: b"value".to_vec(),
         expire_at_ms: Some(123),
      };
      let expected = {
         let coordinator = ReplicationCoordinator::new(16);
         coordinator.open_journal(&path_str, [9; 16]).unwrap();
         assert_eq!(coordinator.append(record.clone()).unwrap(), 1);
         coordinator.flush_journal();
         let mut expected = record;
         expected.offset = 1;
         encode_mutation(&expected).unwrap()
      };
      // The journal is the 21-byte header followed by one length-prefixed
      // mutation that must match the replication wire encoding byte for byte.
      let bytes = std::fs::read(&path).unwrap();
      let body = &bytes[21..];
      assert_eq!(
         u32::from_le_bytes(body[..4].try_into().unwrap()) as usize,
         expected.len()
      );
      assert_eq!(&body[4..], &expected[..]);

      let reopened = ReplicationCoordinator::new(16);
      reopened.open_journal(&path_str, [9; 16]).unwrap();
      assert_eq!(reopened.next_offset(), 2);
      let replayed = reopened.replay_from(1).unwrap();
      assert_eq!(replayed.len(), 1);
      assert_eq!(replayed[0].offset, 1);
      assert_eq!(replayed[0].slot, Slot(42));
      assert_eq!(replayed[0].key, b"key");
      assert_eq!(replayed[0].value, b"value");
      assert_eq!(replayed[0].expire_at_ms, Some(123));
      let _ = std::fs::remove_file(path);
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
      // Retained entries resolve by arithmetic; evicted and future ones do not.
      assert_eq!(log.record_at(2).unwrap().unwrap().offset, 2);
      assert_eq!(log.record_at(3).unwrap().unwrap().offset, 3);
      assert!(log.record_at(4).unwrap().is_none());
      // Offset 0 means "from the beginning"; the first retained entry is 2.
      assert!(log.record_at(0).unwrap().is_none());
      assert_eq!(log.record_at(1), Err(LogError::OffsetGap));
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
         stream.apply(&ReplicationMessage::Entry(Arc::new(MutationRecord {
            offset: 1,
            slot: Slot(1),
            kind: MutationKind::Delete,
            key: b"k".to_vec(),
            value: Vec::new(),
            expire_at_ms: None
         }))),
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
