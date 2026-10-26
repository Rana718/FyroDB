use std::collections::{HashMap, VecDeque};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::thread;
use std::time::{Duration, Instant};

use super::health::PeerHealth;
use super::{
   Frame, FrameCodec, MessageType, PeerConnection, PeerHealthSnapshot, RequestError,
   RequestRegistry, decode_topology,
};
use crate::cluster::server::stable_id;
use crate::cluster::{
   ClusterConfig, ClusterState, NodeInfo, ReplicationCoordinator, ReplicationMessage,
   decode_failure_report, decode_replication_message, encode_replication_message,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const MIN_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);
const SNAPSHOT_START: u16 = 1;
const SNAPSHOT_END: u16 = 2;
const SNAPSHOT_CHUNK: usize = 1024 * 1024;

/// Owns exactly one bounded outbound queue and connection worker per peer.
/// Dropping the manager closes the queues and lets the workers exit.
pub struct PeerManager {
   /// Keyed by `stable_id` so a per-mutation send hashes a short numeric key
   /// instead of the peer's string id.
   peers: HashMap<u64, PeerHandle>,
   next_request_id: AtomicU64,
   _workers: Vec<thread::JoinHandle<()>>,
   state: ClusterState,
   queue_full_count: AtomicU64,
   reconnect_count: Arc<AtomicU64>,
}

struct PeerHandle {
   id: String,
   sender: SyncSender<Frame>,
   requests: RequestRegistry,
   health: PeerHealth,
   replication_lag: Arc<AtomicU64>,
}

const SNAPSHOT_WINDOW: usize = 8;
const REPLICATION_WINDOW: usize = 64;
const MIGRATE_BATCH_RECORDS: usize = 128;
const MIGRATE_BATCH_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerSendError {
   UnknownPeer,
   QueueFull,
   Disconnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationSendError {
   UnknownPeer,
   QueueFull,
   Disconnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerRequestError {
   UnknownPeer,
   QueueFull,
   Disconnected,
   Codec,
   Registry(RequestError),
}

impl PeerManager {
   fn peer(&self, peer_id: &str) -> Option<&PeerHandle> {
      self.peers.get(&stable_id(peer_id))
   }

   pub fn try_send(&self, peer_id: &str, frame: Frame) -> Result<(), PeerSendError> {
      let sender = &self.peer(peer_id).ok_or(PeerSendError::UnknownPeer)?.sender;
      sender.try_send(frame).map_err(|error| match error {
         TrySendError::Full(_) => {
            self.queue_full_count.fetch_add(1, Ordering::Relaxed);
            PeerSendError::QueueFull
         }
         TrySendError::Disconnected(_) => PeerSendError::Disconnected,
      })
   }

   pub fn try_send_replication(
      &self,
      peer_id: &str,
      frame: Frame,
   ) -> Result<(), ReplicationSendError> {
      self.try_send(peer_id, frame).map_err(|error| match error {
         PeerSendError::UnknownPeer => ReplicationSendError::UnknownPeer,
         PeerSendError::QueueFull => ReplicationSendError::QueueFull,
         PeerSendError::Disconnected => ReplicationSendError::Disconnected,
      })
   }

   /// Register and queue a request without waiting for its reply. Pipelined
   /// callers pair this with [`PeerManager::wait_request`].
   pub fn send_request(&self, peer_id: &str, mut frame: Frame) -> Result<u64, PeerRequestError> {
      let peer = self.peer(peer_id).ok_or(PeerRequestError::UnknownPeer)?;
      let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
      frame.request_id = request_id;
      peer
         .requests
         .register(request_id)
         .map_err(PeerRequestError::Registry)?;
      if let Err(error) = peer.sender.try_send(frame) {
         let _ = peer.requests.cancel(request_id);
         return Err(match error {
            TrySendError::Full(_) => {
               self.queue_full_count.fetch_add(1, Ordering::Relaxed);
               PeerRequestError::QueueFull
            }
            TrySendError::Disconnected(_) => PeerRequestError::Disconnected,
         });
      }
      Ok(request_id)
   }

   pub fn wait_request(
      &self,
      peer_id: &str,
      request_id: u64,
      timeout: Duration,
   ) -> Result<Frame, PeerRequestError> {
      let peer = self.peer(peer_id).ok_or(PeerRequestError::UnknownPeer)?;
      peer
         .requests
         .wait(request_id, timeout)
         .map_err(PeerRequestError::Registry)
   }

   pub fn cancel_request(&self, peer_id: &str, request_id: u64) {
      if let Some(peer) = self.peer(peer_id) {
         let _ = peer.requests.cancel(request_id);
      }
   }

   pub fn request(
      &self,
      peer_id: &str,
      frame: Frame,
      timeout: Duration,
   ) -> Result<Frame, PeerRequestError> {
      let request_id = self.send_request(peer_id, frame)?;
      self.wait_request(peer_id, request_id, timeout)
   }

   pub fn peer_count(&self) -> usize {
      self.peers.len()
   }

   pub fn peer_ids(&self) -> Vec<String> {
      self.peers.values().map(|peer| peer.id.clone()).collect()
   }

   pub fn health(&self, peer_id: &str) -> Option<PeerHealthSnapshot> {
      self.peer(peer_id).map(|peer| peer.health.snapshot())
   }

   pub fn record_failure_report(&self, payload: &[u8]) -> bool {
      decode_failure_report(payload).is_some_and(|report| self.state.record_failure(report))
   }

   pub fn suspect_peers(&self, timeout: Duration) -> Vec<String> {
      self
         .peers
         .values()
         .filter(|peer| peer.health.is_suspect(timeout))
         .map(|peer| peer.id.clone())
         .collect()
   }

   pub fn transport_counters(&self) -> (u64, u64) {
      (
         self.queue_full_count.load(Ordering::Relaxed),
         self.reconnect_count.load(Ordering::Relaxed),
      )
   }

   pub fn replication_lag_totals(&self) -> (u64, u64) {
      self.peers.values().fold((0, 0), |(total, max), peer| {
         let lag = peer.replication_lag.load(Ordering::Relaxed);
         (total.saturating_add(lag), max.max(lag))
      })
   }

   pub fn migrate_slot(
      &self,
      store: &crate::storage::store::Store,
      slot: crate::cluster::Slot,
      target_id: &str,
      timeout: Duration,
   ) -> Result<usize, PeerRequestError> {
      let state = store.cluster_state();
      if !state.begin_slot_migration(slot, target_id.to_owned()) {
         return Err(PeerRequestError::UnknownPeer);
      }
      let epoch = state.topology().epoch;
      let frame = |message_type, payload| Frame {
         message_type,
         flags: 0,
         request_id: 0,
         source_id: stable_id(&store.cluster.local_id),
         target_id: stable_id(target_id),
         epoch,
         payload,
      };
      self.request(
         target_id,
         frame(
            MessageType::MigrateBegin,
            slot.value().to_be_bytes().to_vec(),
         ),
         timeout,
      )?;
      // One drain is enough: once the slot is marked migrating, new writes to
      // it are ASK-redirected, so the scan below cannot race a local write.
      store.drain_writes();
      let mut sent = 0usize;
      let mut failed: Option<PeerRequestError> = None;
      let mut batch: Vec<crate::cluster::MutationRecord> =
         Vec::with_capacity(MIGRATE_BATCH_RECORDS);
      let mut batch_bytes = 0usize;
      {
         let flush = |batch: &mut Vec<crate::cluster::MutationRecord>,
                      batch_bytes: &mut usize,
                      sent: &mut usize,
                      failed: &mut Option<PeerRequestError>| {
            if batch.is_empty() {
               return;
            }
            let result = crate::cluster::encode_mutation_batch(batch)
               .map_err(|_| PeerRequestError::Codec)
               .and_then(|payload| {
                  self.request(
                     target_id,
                     frame(MessageType::MigrateChunk, payload),
                     timeout,
                  )
               });
            match result {
               Ok(_) => *sent += batch.len(),
               Err(error) => *failed = Some(error),
            }
            batch.clear();
            *batch_bytes = 0;
         };
         store.for_each_slot_record(slot, |record| {
            if failed.is_some() {
               return;
            }
            batch_bytes = batch_bytes.saturating_add(record.key.len() + record.value.len());
            batch.push(record);
            if batch.len() >= MIGRATE_BATCH_RECORDS || batch_bytes >= MIGRATE_BATCH_BYTES {
               flush(&mut batch, &mut batch_bytes, &mut sent, &mut failed);
            }
         });
         flush(&mut batch, &mut batch_bytes, &mut sent, &mut failed);
      }
      if let Some(error) = failed {
         return Err(error);
      }
      self.request(
         target_id,
         frame(
            MessageType::MigrateFinish,
            slot.value().to_be_bytes().to_vec(),
         ),
         timeout,
      )?;
      // Remove the source only after full ack: a failed partial transfer stays
      // authoritative and retryable.
      store.remove_slot_values(slot);
      if let Some(topology) = state.commit_slot_migration(slot) {
         let _ = store.install_cluster_topology(topology.clone());
         if let Ok(payload) = super::encode_topology(&topology) {
            for peer_id in self.peer_ids() {
               let _ = self.try_send(
                  &peer_id,
                  Frame {
                     message_type: MessageType::Topology,
                     flags: 0,
                     request_id: 0,
                     source_id: stable_id(&store.cluster.local_id),
                     target_id: stable_id(&peer_id),
                     epoch: topology.epoch,
                     payload: payload.clone(),
                  },
               );
            }
         }
      }
      Ok(sent)
   }
}

pub fn start_peer_manager(config: &ClusterConfig, state: ClusterState) -> PeerManager {
   let mut peers = HashMap::new();
   let mut workers = Vec::new();
   let reconnect_count = Arc::new(AtomicU64::new(0));
   for node in config
      .topology
      .nodes
      .iter()
      .filter(|node| node.id != config.local_id)
   {
      let (sender, receiver) = mpsc::sync_channel(config.peer_queue_capacity);
      let requests = RequestRegistry::new(config.peer_queue_capacity);
      let health = PeerHealth::new();
      let replication_lag = Arc::new(AtomicU64::new(0));
      peers.insert(
         stable_id(&node.id),
         PeerHandle {
            id: node.id.clone(),
            sender,
            requests: requests.clone(),
            health: health.clone(),
            replication_lag,
         },
      );
      let local_id = config.local_id.clone();
      let local_epoch = config.topology.epoch;
      let heartbeat_interval = config.heartbeat_interval;
      let auth_token = config.auth_token.clone();
      let worker_reconnect_count = Arc::clone(&reconnect_count);
      let node = node.clone();
      if let Ok(worker) = thread::Builder::new()
         .name(format!("fyrodb-cluster-out-{}", node.id))
         .stack_size(64 * 1024)
         .spawn(move || {
            let ctx = PeerWorkerContext {
               local_id: &local_id,
               epoch: local_epoch,
               heartbeat_interval,
               auth_token: auth_token.as_deref(),
            };
            run_peer_worker(
               receiver,
               requests,
               health,
               &ctx,
               &node,
               worker_reconnect_count,
            )
         })
      {
         workers.push(worker);
      }
   }
   PeerManager {
      peers,
      next_request_id: AtomicU64::new(1),
      _workers: workers,
      state,
      queue_full_count: AtomicU64::new(0),
      reconnect_count,
   }
}

/// One bounded ACK-driven stream per replica; one mutation outside the
/// retained log, so memory is independent of lag.
pub fn start_replication_streams(
   config: &ClusterConfig,
   manager: Arc<PeerManager>,
   store: Arc<crate::storage::store::Store>,
   log: ReplicationCoordinator,
) {
   let local_id = config.local_id.clone();
   let epoch = config.topology.epoch;
   let timeout = config.suspect_timeout;
   for replica in config.topology.nodes.iter().filter(|node| {
      node.role == crate::cluster::NodeRole::Replica
         && node.replica_of.as_deref() == Some(local_id.as_str())
   }) {
      let replica_id = replica.id.clone();
      let manager = Arc::clone(&manager);
      let log = log.clone();
      let store = Arc::clone(&store);
      let local_id = local_id.clone();
      let _ = thread::Builder::new()
            .name(format!("fyrodb-repl-{replica_id}"))
            .stack_size(64 * 1024)
            .spawn(move || {
                let identity = log.identity().unwrap_or([0; 16]);
                // A window of in-flight entries keeps the socket busy instead
                // of paying a full request/reply round trip per mutation.
                let mut next_to_send = 0u64;
                let mut next_to_ack = 0u64;
                let mut pending: VecDeque<(u64, u64)> = VecDeque::new();
                loop {
                    if next_to_send == 0 {
                        pending.clear();
                        let mut begin = Vec::with_capacity(33);
                        begin.push(1);
                        begin.extend_from_slice(&epoch.to_be_bytes());
                        begin.extend_from_slice(&store.replica_applied_offset().to_be_bytes());
                        begin.extend_from_slice(&identity);
                        let resumed = manager.request(&replica_id, Frame { message_type: MessageType::ReplicationBegin, flags: 0, request_id: 0, source_id: stable_id(&local_id), target_id: stable_id(&replica_id), epoch, payload: begin }, timeout)
                            .ok()
                            .and_then(|frame| {
                                if frame.payload.len() != 24 || frame.payload[..16] != identity { return None; }
                                Some(u64::from_be_bytes(frame.payload[16..24].try_into().unwrap()))
                            })
                            .filter(|offset| *offset == 0 || log.record_at(offset.saturating_add(1)).is_ok())
                            .map(|offset| offset.saturating_add(1));
                        if let Some(offset) = resumed {
                            next_to_send = offset.max(1);
                            next_to_ack = next_to_send;
                        } else if let Some(snapshot_offset) = send_snapshot(&manager, &replica_id, &local_id, epoch, timeout, &store, &log) {
                            next_to_send = snapshot_offset.saturating_add(1);
                            next_to_ack = next_to_send;
                        } else {
                            thread::sleep(Duration::from_millis(100));
                        }
                        continue;
                    }

                    let mut fell_behind = false;
                    while pending.len() < REPLICATION_WINDOW {
                        let record = match log.record_at(next_to_send) {
                            Ok(Some(record)) => record,
                            Ok(None) => break,
                            Err(_) => {
                                fell_behind = true;
                                break;
                            }
                        };
                        let offset = record.offset;
                        let Ok(payload) =
                            encode_replication_message(&ReplicationMessage::Entry(record))
                        else {
                            break;
                        };
                        match manager.send_request(
                            &replica_id,
                            Frame {
                                message_type: MessageType::ReplicationEntry,
                                flags: 0,
                                request_id: 0,
                                source_id: stable_id(&local_id),
                                target_id: stable_id(&replica_id),
                                epoch,
                                payload,
                            },
                        ) {
                            Ok(request_id) => {
                                pending.push_back((request_id, offset));
                                next_to_send = offset.saturating_add(1);
                            }
                            Err(PeerRequestError::QueueFull) => break,
                            Err(_) => break,
                        }
                    }

                    if fell_behind {
                        for (request_id, _) in pending.drain(..) {
                            manager.cancel_request(&replica_id, request_id);
                        }
                        eprintln!(
                            "[cluster] replica {replica_id} requires snapshot catch-up (requested {}, retained from {})",
                            next_to_send,
                            log.first_retained_offset()
                        );
                        if let Some(snapshot_offset) = send_snapshot(
                            &manager,
                            &replica_id,
                            &local_id,
                            epoch,
                            timeout,
                            &store,
                            &log,
                        ) {
                            next_to_send = snapshot_offset.saturating_add(1);
                            next_to_ack = next_to_send;
                        }
                        thread::sleep(Duration::from_millis(100));
                        continue;
                    }

                    let Some(&(request_id, record_offset)) = pending.front() else {
                        thread::sleep(Duration::from_millis(10));
                        continue;
                    };
                    match manager.wait_request(&replica_id, request_id, timeout) {
                        Ok(response) => {
                            pending.pop_front();
                            if let Ok(ReplicationMessage::Ack { applied_offset }) =
                                decode_replication_message(&response.payload)
                                && applied_offset >= record_offset
                            {
                                if let Some(peer) = manager.peer(&replica_id) {
                                    peer.replication_lag.store(
                                        log.next_offset()
                                            .saturating_sub(applied_offset.saturating_add(1)),
                                        Ordering::Relaxed,
                                    );
                                }
                                next_to_ack = applied_offset.saturating_add(1);
                            }
                        }
                        Err(_) => {
                            // Rewind to the last applied offset; the replica is
                            // idempotent, so re-sending is safe.
                            for (request_id, _) in pending.drain(..) {
                                manager.cancel_request(&replica_id, request_id);
                            }
                            next_to_send = next_to_ack.max(1);
                            thread::sleep(Duration::from_millis(100));
                        }
                    }
                }
            });
   }
}

fn send_snapshot(
   manager: &PeerManager,
   replica_id: &str,
   local_id: &str,
   epoch: u64,
   timeout: Duration,
   store: &crate::storage::store::Store,
   log: &ReplicationCoordinator,
) -> Option<u64> {
   store.record_snapshot_attempt();
   let path = std::env::temp_dir().join(format!("fyrodb-repl-{local_id}-{replica_id}.rdb"));
   let write_guard = store.cluster_write_guard();
   if crate::storage::rdb::save(store, path.to_str().unwrap_or("")).is_err() {
      return None;
   }
   let snapshot_offset = log.next_offset().saturating_sub(1);
   drop(write_guard);
   let Ok(mut file) = std::fs::File::open(&path) else {
      return None;
   };
   let Ok(file_len) = file.metadata().map(|meta| meta.len()) else {
      return None;
   };
   let mut chunk = vec![0u8; SNAPSHOT_CHUNK];
   let mut pending: VecDeque<u64> = VecDeque::new();
   let mut first = true;
   let mut sent = 0u64;
   let mut ended = false;
   // Keep several chunks in flight; a 1 MB bootstrap used to pay one round
   // trip per chunk.
   let mut stream = || -> Option<()> {
      loop {
         while !ended && pending.len() < SNAPSHOT_WINDOW {
            let read = std::io::Read::read(&mut file, &mut chunk).ok()?;
            if read == 0 {
               break;
            }
            sent = sent.saturating_add(read as u64);
            let end = sent == file_len;
            let payload =
               crate::cluster::encode_snapshot(epoch, snapshot_offset, &chunk[..read]).ok()?;
            let request_id = manager
               .send_request(
                  replica_id,
                  Frame {
                     message_type: MessageType::ReplicationSnapshot,
                     flags: (if first { SNAPSHOT_START } else { 0 })
                        | (if end { SNAPSHOT_END } else { 0 }),
                     request_id: 0,
                     source_id: stable_id(local_id),
                     target_id: stable_id(replica_id),
                     epoch,
                     payload,
                  },
               )
               .ok()?;
            pending.push_back(request_id);
            first = false;
            if end {
               ended = true;
            }
         }
         let Some(request_id) = pending.pop_front() else {
            break;
         };
         let response = manager.wait_request(replica_id, request_id, timeout).ok()?;
         let ReplicationMessage::Ack { applied_offset } =
            decode_replication_message(&response.payload).ok()?
         else {
            return None;
         };
         if ended && pending.is_empty() && applied_offset != snapshot_offset {
            return None;
         }
      }
      ended.then_some(())
   };
   if stream().is_none() {
      // Un-waited requests would otherwise sit in the registry until the peer
      // times them out, starving later snapshot and replication requests.
      for request_id in pending.drain(..) {
         manager.cancel_request(replica_id, request_id);
      }
      let _ = std::fs::remove_file(&path);
      return None;
   }
   let _ = std::fs::remove_file(path);
   Some(snapshot_offset)
}

pub fn start_health_monitor(
   config: ClusterConfig,
   manager: Arc<PeerManager>,
   store: Arc<crate::storage::store::Store>,
   state: ClusterState,
) {
   let _ = thread::Builder::new()
      .name("fyrodb-cluster-health".into())
      .stack_size(64 * 1024)
      .spawn(move || {
         let mut reported = HashMap::<String, Instant>::new();
         loop {
            let total = manager.peer_count();
            let mut healthy = 0usize;
            let mut suspect = 0usize;
            for node in config
               .topology
               .nodes
               .iter()
               .filter(|node| node.id != config.local_id)
            {
               let is_suspect = manager
                  .health(&node.id)
                  .is_none_or(|health| health.state == super::health::PeerState::Disconnected);
               if is_suspect {
                  suspect += 1;
                  let due = reported
                     .get(&node.id)
                     .is_none_or(|last| last.elapsed() >= config.suspect_timeout);
                  if due {
                     let report = crate::cluster::FailureReport {
                        target_id: node.id.clone(),
                        reporter_id: config.local_id.clone(),
                        epoch: config.topology.epoch,
                     };
                     // Count own observation first, or quorum can never be reached.
                     let confirmed = state.record_failure(report.clone());
                     let promoted = if confirmed {
                        state.promote_replica(&report.target_id, report.epoch)
                     } else {
                        None
                     };
                     let payload =
                        crate::cluster::encode_failure_report(&report).unwrap_or_default();
                     // Voters need the reports; the failed target cannot help form quorum.
                     for voter_id in manager
                        .peer_ids()
                        .into_iter()
                        .filter(|voter_id| voter_id != &node.id)
                     {
                        let _ = manager.try_send(
                           &voter_id,
                           Frame {
                              message_type: MessageType::FailureReport,
                              flags: 0,
                              request_id: 0,
                              source_id: stable_id(&config.local_id),
                              target_id: stable_id(&voter_id),
                              epoch: config.topology.epoch,
                              payload: payload.clone(),
                           },
                        );
                     }
                     if let Some(topology) = promoted
                        && let Ok(topology_payload) = super::encode_topology(&topology)
                     {
                        let _ = store.install_cluster_topology(topology.clone());
                        for peer_id in manager.peer_ids() {
                           let _ = manager.try_send(
                              &peer_id,
                              Frame {
                                 message_type: MessageType::Topology,
                                 flags: 0,
                                 request_id: 0,
                                 source_id: stable_id(&config.local_id),
                                 target_id: stable_id(&peer_id),
                                 epoch: topology.epoch,
                                 payload: topology_payload.clone(),
                              },
                           );
                        }
                     }
                     reported.insert(node.id.clone(), Instant::now());
                  }
               } else if manager
                  .health(&node.id)
                  .is_some_and(|health| health.state == super::health::PeerState::Healthy)
               {
                  healthy += 1;
                  reported.remove(&node.id);
               }
            }
            store.update_cluster_health(total, healthy, suspect);
            let (queue_full, reconnects) = manager.transport_counters();
            store.update_cluster_transport_metrics(queue_full, reconnects);
            let (lag_total, lag_max) = manager.replication_lag_totals();
            store.update_cluster_replication_lag(lag_total, lag_max);
            thread::sleep(config.heartbeat_interval);
         }
      });
}

/// Local-node identity the outbound worker needs: who we are, the topology
/// epoch we speak, how often to heartbeat, and how to authenticate.
struct PeerWorkerContext<'a> {
   local_id: &'a str,
   epoch: u64,
   heartbeat_interval: Duration,
   auth_token: Option<&'a str>,
}

fn run_peer_worker(
   receiver: mpsc::Receiver<Frame>,
   requests: RequestRegistry,
   health: PeerHealth,
   ctx: &PeerWorkerContext<'_>,
   node: &NodeInfo,
   reconnect_count: Arc<AtomicU64>,
) {
   let mut peer = None;
   let mut backoff = MIN_BACKOFF;
   let mut request_id = 1u64;
   let mut connected_once = false;
   let mut batch: Vec<Frame> = Vec::with_capacity(64);
   let mut scratch: Vec<[u8; 48]> = Vec::new();
   loop {
      batch.clear();
      let first = match receiver.recv_timeout(ctx.heartbeat_interval) {
         Ok(frame) => frame,
         Err(mpsc::RecvTimeoutError::Timeout) => {
            let frame = Frame {
               message_type: MessageType::Ping,
               flags: 0,
               request_id,
               source_id: stable_id(ctx.local_id),
               target_id: stable_id(&node.id),
               epoch: ctx.epoch,
               payload: Vec::new(),
            };
            request_id = request_id.wrapping_add(1).max(1);
            frame
         }
         Err(mpsc::RecvTimeoutError::Disconnected) => break,
      };
      batch.push(first);
      // Drain everything already queued so a replication burst leaves in
      // one writev instead of one syscall per frame.
      while batch.len() < 256 {
         match receiver.try_recv() {
            Ok(frame) => batch.push(frame),
            Err(_) => break,
         }
      }
      loop {
         if peer.is_none() {
            health.connecting();
            // Per-attempt resolution: hostnames can change IP on node restart.
            let candidates = resolve_peer_address(&node.cluster_address, &node.id);
            if candidates.is_empty() {
               health.disconnected(None);
               thread::sleep(backoff);
               backoff = backoff.saturating_mul(2).min(MAX_BACKOFF);
               continue;
            }
            // Try every candidate: dual-stack names often yield IPv6 first.
            let attempt = candidates.iter().find_map(|&address| {
               connect_and_handshake(address, ctx.local_id, node, ctx.epoch, ctx.auth_token).ok()
            });
            match attempt {
               Some(connection) => {
                  if connected_once {
                     reconnect_count.fetch_add(1, Ordering::Relaxed);
                  }
                  connected_once = true;
                  let generation = health.connected();
                  if let Ok(reader) = connection.try_clone() {
                     let pending = requests.clone();
                     let peer_health = health.clone();
                     thread::Builder::new()
                        .name(format!("fyrodb-cluster-in-{}", node.id))
                        .stack_size(64 * 1024)
                        .spawn(move || read_replies(reader, pending, peer_health, generation))
                        .ok();
                  }
                  peer = Some(connection);
                  backoff = MIN_BACKOFF;
               }
               None => {
                  health.disconnected(None);
                  thread::sleep(backoff);
                  backoff = backoff.saturating_mul(2).min(MAX_BACKOFF);
                  continue;
               }
            }
         }
         if peer
            .as_mut()
            .is_some_and(|connection| connection.send_batch(&batch, &mut scratch).is_ok())
         {
            break;
         }
         peer = None;
         health.disconnected(None);
         if backoff < MAX_BACKOFF {
            thread::sleep(backoff);
            backoff = backoff.saturating_mul(2).min(MAX_BACKOFF);
         }
      }
   }
}

fn read_replies(
   mut peer: PeerConnection,
   requests: RequestRegistry,
   health: PeerHealth,
   generation: u64,
) {
   while let Ok(frame) = peer.receive() {
      if frame.message_type == MessageType::Pong {
         health.pong(generation);
      }
      if matches!(
         frame.message_type,
         MessageType::CommandReply | MessageType::Pong | MessageType::ReplicationAck
      ) {
         let _ = requests.complete(frame);
      }
   }
   health.disconnected(Some(generation));
   requests.fail_pending();
}

/// Accepts hostnames, not just ip:port; returns all candidates because
/// resolution order is not connectability order.
fn resolve_peer_address(address: &str, node_id: &str) -> Vec<SocketAddr> {
   if let Ok(parsed) = address.parse::<SocketAddr>() {
      return vec![parsed];
   }
   match address.to_socket_addrs() {
      Ok(resolved) => {
         let candidates: Vec<SocketAddr> = resolved.collect();
         if candidates.is_empty() {
            eprintln!("fyrodb: cluster address {address} for node {node_id} resolved to nothing");
         }
         candidates
      }
      Err(error) => {
         eprintln!("fyrodb: cannot resolve cluster address {address} for node {node_id}: {error}");
         Vec::new()
      }
   }
}

fn connect_and_handshake(
   address: SocketAddr,
   local_id: &str,
   node: &NodeInfo,
   epoch: u64,
   auth_token: Option<&str>,
) -> Result<PeerConnection, super::ProtocolError> {
   let mut peer = PeerConnection::connect(address, CONNECT_TIMEOUT, FrameCodec::default())?;
   peer.send(&Frame {
      message_type: MessageType::Hello,
      flags: 0,
      request_id: 0,
      source_id: stable_id(local_id),
      target_id: stable_id(&node.id),
      epoch,
      payload: match auth_token {
         Some(token) => format!("{token}\0{local_id}").into_bytes(),
         None => local_id.as_bytes().to_vec(),
      },
   })?;
   let response = peer.receive()?;
   if response.message_type != MessageType::Hello || response.payload != node.id.as_bytes() {
      return Err(super::ProtocolError::InvalidHandshake);
   }
   let topology = peer.receive()?;
   if topology.message_type != MessageType::Topology || topology.epoch < epoch {
      return Err(super::ProtocolError::InvalidHandshake);
   }
   decode_topology(&topology.payload).map_err(|_| super::ProtocolError::InvalidHandshake)?;
   peer.set_read_timeout(None)?;
   Ok(peer)
}

#[cfg(test)]
mod tests {
   use super::*;
   use crate::cluster::{NodeRole, Slot, SlotRange, Topology};

   fn config() -> ClusterConfig {
      let node = |id: &str, port| NodeInfo {
         id: id.into(),
         address: format!("127.0.0.1:{port}"),
         cluster_address: format!("127.0.0.1:{}", port + 10_000),
         role: NodeRole::Primary,
         replica_of: None,
         epoch: 1,
         slots: vec![SlotRange::new(Slot(0), Slot(1)).unwrap()],
      };
      ClusterConfig {
         enabled: true,
         local_id: "a".into(),
         listen_address: "127.0.0.1:18000".into(),
         peer_queue_capacity: 16,
         heartbeat_interval: Duration::from_secs(2),
         suspect_timeout: Duration::from_secs(6),
         failure_quorum: 2,
         max_inbound_peers: 16,
         auth_token: None,
         replication_log_capacity: 16,
         is_replica: false,
         nodes_config_file: String::new(),
         topology: Topology::new(1, vec![node("a", 8000), node("b", 8001)]),
      }
   }

   #[test]
   fn creates_only_remote_peer_queues() {
      let manager = start_peer_manager(&config(), ClusterState::new(2, Duration::from_secs(30)));
      assert_eq!(manager.peer_count(), 1);
      let frame = Frame {
         message_type: MessageType::Ping,
         flags: 0,
         request_id: 1,
         source_id: 1,
         target_id: 2,
         epoch: 1,
         payload: Vec::new(),
      };
      assert_eq!(
         manager.try_send("missing", frame),
         Err(PeerSendError::UnknownPeer)
      );
   }

   /// Regression: hostname cluster-bus addresses must resolve to socket
   /// addresses; bare `parse::<SocketAddr>()` rejects them.
   #[test]
   fn hostname_cluster_addresses_resolve() {
      let candidates = super::resolve_peer_address("localhost:19301", "node-1");
      assert!(
         !candidates.is_empty(),
         "a hostname must resolve to at least one candidate"
      );
      assert!(candidates.iter().all(|addr| addr.port() == 19301));
      assert!(
         "localhost:19301".parse::<std::net::SocketAddr>().is_err(),
         "this is the parse that used to silently disable the bus"
      );
   }

   /// Dual-stack names yield IPv6 first; the node may bind IPv4 only.
   #[test]
   fn every_resolved_candidate_is_returned() {
      let candidates = super::resolve_peer_address("localhost:19301", "node-1");
      let has_v4 = candidates.iter().any(|addr| addr.is_ipv4());
      let has_v6 = candidates.iter().any(|addr| addr.is_ipv6());
      assert!(
         has_v4 || has_v6,
         "expected at least one address family for localhost"
      );
      if candidates.len() > 1 {
         assert!(
            has_v4 && has_v6,
            "a multi-candidate localhost should expose both families"
         );
      }
   }

   #[test]
   fn numeric_addresses_skip_resolution() {
      assert_eq!(
         super::resolve_peer_address("10.0.0.7:18000", "node-1"),
         vec!["10.0.0.7:18000".parse::<std::net::SocketAddr>().unwrap()]
      );
   }

   #[test]
   fn unresolvable_addresses_yield_no_candidates() {
      assert!(super::resolve_peer_address("no-such-host.invalid:18000", "node-1").is_empty());
      assert!(super::resolve_peer_address("garbage", "node-1").is_empty());
   }
}
