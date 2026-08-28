use std::time::Duration;

use fyro_db::cluster::{
    ClusterConfig, Frame, FrameCodec, MessageType, MutationKind, MutationLog, MutationRecord,
    NodeInfo, NodeRole, ReplicationStream, RouteDecision, Slot, SlotRange, Topology,
    decode_replication_message, encode_replication_message, hash_slot, route_command,
};

fn config() -> ClusterConfig {
    ClusterConfig {
        enabled: true,
        local_id: "node-a".into(),
        listen_address: "127.0.0.1:18000".into(),
        topology: Topology::new(
            1,
            vec![
                NodeInfo {
                    id: "node-a".into(),
                    address: "127.0.0.1:8000".into(),
                    cluster_address: "127.0.0.1:18000".into(),
                    role: NodeRole::Primary,
                    replica_of: None,
                    epoch: 1,
                    slots: vec![SlotRange::new(Slot(0), Slot(8191)).unwrap()],
                },
                NodeInfo {
                    id: "node-b".into(),
                    address: "127.0.0.1:8001".into(),
                    cluster_address: "127.0.0.1:18001".into(),
                    role: NodeRole::Primary,
                    replica_of: None,
                    epoch: 1,
                    slots: vec![SlotRange::new(Slot(8192), Slot(16383)).unwrap()],
                },
            ],
        ),
        peer_queue_capacity: 16,
        heartbeat_interval: Duration::from_secs(2),
        suspect_timeout: Duration::from_secs(6),
        failure_quorum: 2,
        max_inbound_peers: 16,
        auth_token: None,
        replication_log_capacity: 16,
        is_replica: false,
        nodes_config_file: String::new(),
    }
}

#[test]
fn routes_local_remote_cross_slot_and_no_key_commands() {
    let cluster = config();
    let remote = (0u32..10000)
        .map(|n| n.to_string())
        .find(|key| hash_slot(key.as_bytes()).value() >= 8192)
        .unwrap();
    assert_eq!(route_command(&cluster, b"PING", &[]), RouteDecision::Local);
    assert!(matches!(
        route_command(&cluster, b"GET", &[remote.as_bytes()]),
        RouteDecision::Moved { .. }
    ));
    assert_eq!(
        route_command(&cluster, b"MGET", &[b"local", remote.as_bytes()]),
        RouteDecision::CrossSlot
    );
    assert_eq!(
        route_command(&cluster, b"MSET", &[b"a{same}", b"1", b"b{same}", b"2"]),
        RouteDecision::Moved {
            slot: hash_slot(b"{same}"),
            address: "127.0.0.1:8001"
        }
    );
}

#[test]
fn replication_log_and_stream_are_strict_and_idempotent() {
    let mut log = MutationLog::new(2);
    let record = || MutationRecord {
        offset: 0,
        slot: Slot(1),
        kind: MutationKind::Delete,
        key: b"key".to_vec(),
        value: Vec::new(),
        expire_at_ms: None,
    };
    assert_eq!(log.append(record()).unwrap(), 1);
    assert_eq!(log.append(record()).unwrap(), 2);
    assert_eq!(log.append(record()).unwrap(), 3);
    assert!(log.replay_from(1).is_err());

    let mut entry = record();
    entry.offset = 1;
    let message = fyro_db::cluster::ReplicationMessage::Entry(entry);
    let bytes = encode_replication_message(&message).unwrap();
    assert_eq!(decode_replication_message(&bytes).unwrap(), message);
    let mut stream = ReplicationStream::default();
    stream
        .apply(&fyro_db::cluster::ReplicationMessage::Begin {
            epoch: 1,
            from_offset: 1,
            identity: [7; 16],
        })
        .unwrap();
    assert!(stream.apply(&message).is_ok());
}

#[test]
fn frame_codec_round_trips_cluster_ping() {
    let frame = Frame {
        message_type: MessageType::Ping,
        flags: 0,
        request_id: 1,
        source_id: 2,
        target_id: 3,
        epoch: 4,
        payload: Vec::new(),
    };
    let codec = FrameCodec::default();
    let mut bytes = Vec::new();
    codec.write_frame(&mut bytes, &frame).unwrap();
    assert_eq!(codec.read_frame(&mut bytes.as_slice()).unwrap(), frame);
}

#[test]
fn replication_begin_identity_codec_is_strict() {
    let message = fyro_db::cluster::ReplicationMessage::Begin {
        epoch: 4,
        from_offset: 9,
        identity: [3; 16],
    };
    let encoded = encode_replication_message(&message).unwrap();
    assert_eq!(decode_replication_message(&encoded).unwrap(), message);
    assert!(decode_replication_message(&encoded[..encoded.len() - 1]).is_err());
}
