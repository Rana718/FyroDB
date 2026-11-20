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
   let message = fyro_db::cluster::ReplicationMessage::Entry(std::sync::Arc::new(entry));
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

#[test]
fn routed_string_runs_recheck_each_slot_and_consume_asking_once() {
   use fyro_db::handler::{Conn, net::Stream};
   use fyro_db::pubsub::{PubSub, WorkerNotifier};
   use fyro_db::storage::store::Store;
   use std::sync::Arc;

   let mut base = Store::with_config_workers(2, usize::MAX, 1);
   *base.cluster = config();
   let state = base.cluster_state();
   assert!(state.replace_topology(base.cluster.topology.clone()));
   let local = "bar";
   let remote = "foo";
   let slot = hash_slot(remote.as_bytes());
   assert!(state.begin_slot_import(slot, "node-b".into()));
   let store = Arc::new(base);
   let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
   let client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
   let (mut peer, _) = listener.accept().unwrap();
   client.set_nonblocking(true).unwrap();
   let poll = mio::Poll::new().unwrap();
   let notifier = WorkerNotifier::new(
      Arc::new(mio::Waker::new(poll.registry(), mio::Token(1)).unwrap()),
      0,
   );
   let bus = PubSub::new();
   let mut conn = Conn::new(
      Stream::Tcp(mio::net::TcpStream::from_std(client)),
      &store,
      &bus,
      1,
      &notifier,
      None,
      0,
   );
   let commands: &[&[&str]] = &[
      &["SET", local, "x"],
      &["SET", remote, "y"],
      &["ASKING"],
      &["SET", remote, "z"],
      &["SET", remote, "y"],
      &["GET", local],
      &["GET", remote],
      &["ASKING"],
      &["GET", remote],
      &["GET", remote],
      &["MGET", local, remote],
   ];
   fn send(conn: &mut Conn<'_>, commands: &[&[&str]]) -> Vec<u8> {
      conn.parser.rbuf.clear();
      conn.parser.pos = 0;
      for args in commands {
         conn
            .parser
            .rbuf
            .extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
         for arg in *args {
            conn
               .parser
               .rbuf
               .extend_from_slice(format!("${}\r\n{arg}\r\n", arg.len()).as_bytes());
         }
      }
      conn.parser.filled = conn.parser.rbuf.len();
      assert!(conn.process_buffered());
      std::mem::take(&mut conn.parser.wbuf)
   }
   let replies = send(&mut conn, commands);
   let moved = format!("-MOVED {} 127.0.0.1:8001\r\n", slot.value());
   assert_eq!(replies,format!("+OK\r\n{moved}+OK\r\n+OK\r\n{moved}$1\r\nx\r\n{moved}+OK\r\n$1\r\nz\r\n{moved}-CROSSSLOT Keys in request don't hash to the same slot\r\n").as_bytes());
   store.set_string("expired{bar}", "old", 1);
   let big = "a".repeat(65536);
   store.set_string("big{bar}", &big, 0);
   let replies = send(
      &mut conn,
      &[
         &["GET", local],
         &["GET", "missing{bar}"],
         &["GET", "expired{bar}"],
         &["SET", local, "1"],
         &["SET", local, "2"],
         &["GET", local],
         &["GET", local],
         &["GET", "big{bar}"],
      ],
   );
   let expected =
      format!("$1\r\nx\r\n$-1\r\n$-1\r\n+OK\r\n+OK\r\n$1\r\n2\r\n$1\r\n2\r\n$65536\r\n{big}\r\n");
   assert_eq!(replies, expected.as_bytes());
   assert_eq!(store.dbsize(), 3);

   use std::io::{Read, Write};
   peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
   let bulk_get = b"*2\r\n$3\r\nGET\r\n$8\r\nbig{bar}\r\n";
   let remote_get = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
   peer
      .write_all(
         &[
            bulk_get.as_slice(),
            remote_get,
            bulk_get,
            b"*1\r\n$4\r\nPING\r\n",
         ]
         .concat(),
      )
      .unwrap();
   let expected = format!("$65536\r\n{big}\r\n{moved}$65536\r\n{big}\r\n+PONG\r\n");
   let reader = std::thread::spawn(move || {
      let mut received = vec![0; expected.len()];
      peer.read_exact(&mut received).unwrap();
      assert!(received == expected.as_bytes());
   });
   assert!(conn.do_read());
   while conn.has_pending_write() {
      assert!(conn.do_write());
      std::thread::yield_now();
   }
   reader.join().unwrap();
}

#[test]
fn discovery_uses_live_slots_and_complete_resp2_shard_fields() {
   use fyro_db::commands::connection::cluster_cmd;
   use fyro_db::storage::store::Store;
   let mut store = Store::with_config_workers(2, usize::MAX, 1);
   *store.cluster = config();
   let mut topology = store.cluster.topology.clone();
   topology.epoch += 1;
   topology.nodes[0].slots = vec![SlotRange::new(Slot(0), Slot(16383)).unwrap()];
   topology.nodes[1].role = NodeRole::Replica;
   topology.nodes[1].slots.clear();
   topology.nodes[1].replica_of = Some("node-a".into());
   assert!(store.cluster_state().replace_topology(topology));
   let mut out = Vec::new();
   cluster_cmd(&["CLUSTER", "SLOTS"], &store, &mut out);
   assert!(out.starts_with(b"*1\r\n*3\r\n:0\r\n:16383\r\n"));
   out.clear();
   cluster_cmd(&["CLUSTER", "SHARDS"], &store, &mut out);
   let node = |id, port, role: &str| {
      format!(
         "*8\r\n$2\r\nid\r\n$6\r\n{id}\r\n$8\r\nendpoint\r\n$9\r\n127.0.0.1\r\n$4\r\nport\r\n:{port}\r\n$4\r\nrole\r\n${}\r\n{role}\r\n",
         role.len()
      )
   };
   let expected = format!(
      "*1\r\n*4\r\n$5\r\nslots\r\n*2\r\n:0\r\n:16383\r\n$5\r\nnodes\r\n*2\r\n{}{}",
      node("node-a", 8000, "master"),
      node("node-b", 8001, "replica")
   );
   assert_eq!(out, expected.as_bytes());
}
