use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use super::{
    ClusterConfig, ClusterState, Frame, FrameCodec, MessageType, PeerConnection, ReplicaApplier,
    decode_failure_report, decode_replication_message, encode_replication_message, encode_topology,
};
use crate::storage::store::Store;

struct PeerPermit(Arc<AtomicUsize>);
impl Drop for PeerPermit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Start the dedicated cluster listener. The listener is intentionally
/// separate from client workers so malformed or slow peer traffic cannot
/// consume client connection slots.
pub fn start_listener(
    config: ClusterConfig,
    state: ClusterState,
    store: Arc<Store>,
) -> io::Result<thread::JoinHandle<()>> {
    let Some(_) = config.local_node() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cluster node is not in topology",
        ));
    };
    let address: SocketAddr = config.listen_address.parse().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid cluster address: {error}"),
        )
    })?;
    let listener = TcpListener::bind(address)?;
    listener.set_nonblocking(false)?;
    let local_id = config.local_id.clone();
    let epoch = config.topology.epoch;
    let codec = FrameCodec::default();
    let active_peers = Arc::new(AtomicUsize::new(0));
    let max_inbound_peers = config.max_inbound_peers;
    let peer_timeout = config.suspect_timeout;
    let auth_token = config.auth_token.clone();
    let handle = thread::Builder::new()
        .name("fyrodb-cluster-listener".into())
        .stack_size(64 * 1024)
        .spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        if active_peers.fetch_add(1, Ordering::Relaxed) >= max_inbound_peers {
                            active_peers.fetch_sub(1, Ordering::Relaxed);
                            continue;
                        }
                        let permit = PeerPermit(Arc::clone(&active_peers));
                        let peer_id = local_id.clone();
                        let peer_codec = codec.clone();
                        let peer_topology = state.topology_arc();
                        let peer_state = state.clone();
                        let peer_auth = auth_token.clone();
                        let peer_store = Arc::clone(&store);
                        let peer_timeout = peer_timeout;
                        let _ = thread::Builder::new()
                            .name("fyrodb-cluster-peer".into())
                            .stack_size(64 * 1024)
                            .spawn(move || {
                                let _permit = permit;
                                handle_peer(
                                    stream,
                                    peer_codec,
                                    &peer_id,
                                    epoch,
                                    peer_topology.as_ref(),
                                    &peer_state,
                                    peer_auth.as_deref(),
                                    peer_timeout,
                                    &peer_store,
                                )
                            });
                    }
                    Err(error) => eprintln!("fyrodb cluster accept error: {error}"),
                }
            }
        })?;
    Ok(handle)
}

#[allow(clippy::too_many_arguments)]
fn handle_peer(
    stream: TcpStream,
    codec: FrameCodec,
    local_id: &str,
    epoch: u64,
    topology: &super::Topology,
    state: &ClusterState,
    auth_token: Option<&str>,
    peer_timeout: std::time::Duration,
    store: &Store,
) {
    let Ok(mut peer) = PeerConnection::from_stream(stream, codec) else {
        return;
    };
    if peer.set_read_timeout(Some(peer_timeout)).is_err()
        || peer.set_write_timeout(Some(peer_timeout)).is_err()
    {
        return;
    }
    let Ok(hello) = peer.receive() else { return };
    if hello.message_type != MessageType::Hello || hello.payload.is_empty() {
        return;
    }
    let Some((remote_id, received_token)) = parse_hello(&hello.payload) else {
        return;
    };
    if !auth_matches(auth_token, received_token) {
        return;
    }
    if remote_id == local_id {
        return;
    }
    let response = Frame {
        message_type: MessageType::Hello,
        flags: 0,
        request_id: hello.request_id,
        source_id: stable_id(local_id),
        target_id: hello.source_id,
        epoch,
        payload: local_id.as_bytes().to_vec(),
    };
    if peer.send(&response).is_err() {
        return;
    }
    let Ok(payload) = encode_topology(topology) else {
        return;
    };
    if peer
        .send(&Frame {
            message_type: MessageType::Topology,
            flags: 0,
            request_id: hello.request_id,
            source_id: stable_id(local_id),
            target_id: hello.source_id,
            epoch,
            payload,
        })
        .is_err()
    {
        return;
    }
    let mut replica_applier = ReplicaApplier::new(store.replica_applied_offset());
    let snapshot_path =
        std::env::temp_dir().join(format!("fyrodb-replica-{local_id}-{remote_id}.rdb"));
    let mut snapshot_file: Option<std::fs::File> = None;
    while let Ok(frame) = peer.receive() {
        let current_epoch = state.topology().epoch.max(epoch);
        if frame.epoch < current_epoch {
            continue;
        }
        if frame.epoch > current_epoch && frame.message_type != MessageType::Topology {
            break;
        }
        match frame.message_type {
            MessageType::Ping => {
                let _ = peer.send(&Frame {
                    message_type: MessageType::Pong,
                    flags: 0,
                    request_id: frame.request_id,
                    source_id: stable_id(local_id),
                    target_id: frame.source_id,
                    epoch,
                    payload: Vec::new(),
                });
            }
            MessageType::Hello => {}
            MessageType::FailureReport => {
                if let Some(report) = decode_failure_report(&frame.payload) {
                    // A peer may only attest for itself. This prevents an
                    // authenticated node from forging quorum evidence for a
                    // different reporter or for an unknown target.
                    let known_reporter = report.reporter_id == remote_id
                        && topology
                            .nodes
                            .iter()
                            .any(|node| node.id == report.reporter_id);
                    let known_target = topology
                        .nodes
                        .iter()
                        .any(|node| node.id == report.target_id);
                    if known_reporter && known_target && report.target_id != local_id {
                        let confirmed = state.record_failure(report.clone());
                        if confirmed {
                            if let Some(topology) =
                                state.promote_replica(&report.target_id, report.epoch)
                            {
                                let _ = store.install_cluster_topology(topology);
                            }
                        }
                    }
                }
            }
            MessageType::Topology => {
                if let Ok(received) = super::decode_topology(&frame.payload) {
                    let _ = store.install_cluster_topology(received);
                }
            }
            MessageType::ReplicationEntry => {
                if let Ok(super::ReplicationMessage::Entry(record)) =
                    decode_replication_message(&frame.payload)
                {
                    let source_allowed = topology
                        .nodes
                        .iter()
                        .find(|node| node.id == local_id)
                        .and_then(|node| node.replica_of.as_deref())
                        == Some(remote_id);
                    if source_allowed {
                        if replica_applier
                            .try_apply(&record, |record| store.apply_replica_mutation(record))
                            .is_ok()
                        {
                            store.set_replica_applied_offset(replica_applier.applied_offset());
                            if let Ok(payload) =
                                encode_replication_message(&super::ReplicationMessage::Ack {
                                    applied_offset: replica_applier.applied_offset(),
                                })
                            {
                                let _ = peer.send(&Frame {
                                    message_type: MessageType::ReplicationAck,
                                    flags: 0,
                                    request_id: frame.request_id,
                                    source_id: stable_id(local_id),
                                    target_id: frame.source_id,
                                    epoch,
                                    payload,
                                });
                            }
                        }
                    }
                }
            }
            MessageType::MigrateBegin => {
                if frame.payload.len() != 2 {
                    continue;
                }
                let Some(slot) =
                    super::Slot::new(u16::from_be_bytes([frame.payload[0], frame.payload[1]]))
                else {
                    continue;
                };
                if !state.begin_slot_import(slot, remote_id.to_owned()) {
                    continue;
                }
                let _ = peer.send(&Frame {
                    message_type: MessageType::ReplicationAck,
                    flags: 0,
                    request_id: frame.request_id,
                    source_id: stable_id(local_id),
                    target_id: frame.source_id,
                    epoch: current_epoch,
                    payload: Vec::new(),
                });
            }
            MessageType::MigrateChunk => {
                if let Ok(record) = super::decode_mutation(&frame.payload) {
                    if store.apply_replica_mutation(&record).is_ok() {
                        let _ = peer.send(&Frame {
                            message_type: MessageType::ReplicationAck,
                            flags: 0,
                            request_id: frame.request_id,
                            source_id: stable_id(local_id),
                            target_id: frame.source_id,
                            epoch: current_epoch,
                            payload: Vec::new(),
                        });
                    }
                }
            }
            MessageType::MigrateFinish => {
                if frame.payload.len() != 2 {
                    continue;
                }
                if let Some(slot) =
                    super::Slot::new(u16::from_be_bytes([frame.payload[0], frame.payload[1]]))
                {
                    state.finish_slot_import(slot);
                }
                let _ = peer.send(&Frame {
                    message_type: MessageType::ReplicationAck,
                    flags: 0,
                    request_id: frame.request_id,
                    source_id: stable_id(local_id),
                    target_id: frame.source_id,
                    epoch: current_epoch,
                    payload: Vec::new(),
                });
            }
            MessageType::ReplicationSnapshot => {
                let source_allowed = topology
                    .nodes
                    .iter()
                    .find(|node| node.id == local_id)
                    .and_then(|node| node.replica_of.as_deref())
                    == Some(remote_id);
                if !source_allowed {
                    continue;
                }
                let Ok(super::ReplicationMessage::Snapshot {
                    epoch: snapshot_epoch,
                    offset,
                    payload,
                }) = decode_replication_message(&frame.payload)
                else {
                    continue;
                };
                if snapshot_epoch != epoch {
                    continue;
                }
                if frame.flags & 1 != 0 {
                    snapshot_file = OpenOptions::new()
                        .create(true)
                        .truncate(true)
                        .write(true)
                        .open(&snapshot_path)
                        .ok();
                    store.set_replica_installing(true);
                }
                let Some(file) = snapshot_file.as_mut() else {
                    continue;
                };
                if file.write_all(&payload).is_err() {
                    snapshot_file = None;
                    store.set_replica_installing(false);
                    continue;
                }
                let mut ack_offset = replica_applier.applied_offset();
                if frame.flags & 2 != 0 {
                    let _ = file.flush();
                    let _ = file.sync_all();
                    snapshot_file = None;
                    store.flush();
                    let result = crate::storage::rdb::load_strict(
                        store,
                        snapshot_path.to_str().unwrap_or(""),
                    );
                    let _ = std::fs::remove_file(&snapshot_path);
                    if result.is_ok() {
                        replica_applier.reset(offset);
                        store.set_replica_applied_offset(offset);
                        store.set_replica_installing(false);
                        ack_offset = offset;
                    } else {
                        continue;
                    }
                }
                if let Ok(payload) = encode_replication_message(&super::ReplicationMessage::Ack {
                    applied_offset: ack_offset,
                }) {
                    let _ = peer.send(&Frame {
                        message_type: MessageType::ReplicationAck,
                        flags: 0,
                        request_id: frame.request_id,
                        source_id: stable_id(local_id),
                        target_id: frame.source_id,
                        epoch,
                        payload,
                    });
                }
            }
            MessageType::ReplicationBegin => {
                if let Ok(super::ReplicationMessage::Begin { identity, .. }) =
                    decode_replication_message(&frame.payload)
                {
                    let accepted = store
                        .replica_identity()
                        .is_some_and(|current| current == identity);
                    store.set_replica_identity(identity);
                    let mut payload = Vec::with_capacity(24);
                    payload.extend_from_slice(&identity);
                    payload.extend_from_slice(
                        &(if accepted {
                            store.replica_applied_offset()
                        } else {
                            0
                        })
                        .to_be_bytes(),
                    );
                    let _ = peer.send(&Frame {
                        message_type: MessageType::ReplicationAck,
                        flags: 0,
                        request_id: frame.request_id,
                        source_id: stable_id(local_id),
                        target_id: frame.source_id,
                        epoch,
                        payload,
                    });
                }
            }
            MessageType::ReplicationAck | MessageType::ReplicationFinish => {}
            _ => break,
        }
    }
    if snapshot_file.is_some() {
        store.set_replica_installing(false);
        let _ = std::fs::remove_file(snapshot_path);
    }
}

pub(crate) fn stable_id(id: &str) -> u64 {
    id.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x1000_0000_01b3)
    })
}

fn parse_hello(payload: &[u8]) -> Option<(&str, Option<&str>)> {
    let value = std::str::from_utf8(payload).ok()?;
    value
        .split_once('\0')
        .map_or(Some((value, None)), |(token, id)| Some((id, Some(token))))
}

fn auth_matches(expected: Option<&str>, received: Option<&str>) -> bool {
    match (expected, received) {
        (None, _) => true,
        (Some(expected), Some(received)) => {
            let a = expected.as_bytes();
            let b = received.as_bytes();
            let mut diff = a.len() ^ b.len();
            for index in 0..a.len().max(b.len()) {
                diff |= usize::from(a.get(index) != b.get(index));
            }
            diff == 0
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    use super::{
        ClusterState, Frame, FrameCodec, MessageType, PeerConnection, auth_matches, handle_peer,
        stable_id,
    };
    use crate::storage::store::Store;

    #[test]
    fn stable_ids_are_deterministic() {
        assert_eq!(stable_id("node-a"), stable_id("node-a"));
        assert_ne!(stable_id("node-a"), stable_id("node-b"));
    }

    #[test]
    fn hello_auth_requires_matching_secret() {
        assert!(auth_matches(Some("secret"), Some("secret")));
        assert!(!auth_matches(Some("secret"), Some("wrong")));
        assert!(!auth_matches(Some("secret"), None));
        assert!(auth_matches(None, None));
    }

    #[test]
    #[ignore = "requires loopback socket permissions"]
    fn hello_handshake_and_ping_pong_work() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            handle_peer(
                stream,
                FrameCodec::default(),
                "node-a",
                7,
                &super::super::Topology::default(),
                &ClusterState::new(2, std::time::Duration::from_secs(30)),
                None,
                std::time::Duration::from_secs(6),
                &Store::with_config(1, 16),
            );
        });

        let stream = TcpStream::connect(address).unwrap();
        let mut peer = PeerConnection::from_stream(stream, FrameCodec::default()).unwrap();
        peer.send(&Frame {
            message_type: MessageType::Hello,
            flags: 0,
            request_id: 11,
            source_id: stable_id("node-b"),
            target_id: stable_id("node-a"),
            epoch: 1,
            payload: b"node-b".to_vec(),
        })
        .unwrap();
        let hello = peer.receive().unwrap();
        assert_eq!(hello.message_type, MessageType::Hello);
        assert_eq!(hello.request_id, 11);
        assert_eq!(hello.epoch, 7);
        assert_eq!(hello.payload, b"node-a");
        let topology = peer.receive().unwrap();
        assert_eq!(topology.message_type, MessageType::Topology);

        peer.send(&Frame {
            message_type: MessageType::Ping,
            flags: 0,
            request_id: 12,
            source_id: stable_id("node-b"),
            target_id: stable_id("node-a"),
            epoch: 1,
            payload: Vec::new(),
        })
        .unwrap();
        let pong = peer.receive().unwrap();
        assert_eq!(pong.message_type, MessageType::Pong);
        assert_eq!(pong.request_id, 12);
        drop(peer);
        server.join().unwrap();
    }
}
