//! Cluster metadata and routing primitives.

mod config;
mod failure;
mod hash;
mod replication;
mod routing;
mod server;
mod state;
mod topology;
mod transport;

pub use config::{ClusterConfig, ClusterConfigError, load_nodes_conf, save_nodes_conf};
pub use failure::{FailureReport, FailureTracker, decode_failure_report, encode_failure_report};
pub use hash::{HASH_SLOTS, Slot, SlotRange, hash_slot};
pub use replication::{
    ApplyError, LogError, MutationKind, MutationLog, MutationRecord, ReplicaApplier,
    ReplicationCodecError, ReplicationCoordinator, ReplicationMessage, ReplicationStream,
    StreamError, decode_mutation, decode_replication_message, encode_mutation,
    encode_replication_message,
};
pub use routing::{
    RouteDecision, is_write_command, route_command, route_command_with_state,
    route_command_with_state_import, route_command_with_topology,
};
pub use server::start_listener;
pub use state::ClusterState;
pub use topology::{NodeInfo, NodeRole, Topology};
pub use transport::{
    Frame, FrameCodec, MessageType, PeerConnection, PeerHealthSnapshot, PeerManager,
    PeerRequestError, PeerSendError, PeerState, ProtocolError, ReplicationSendError, RequestError,
    RequestRegistry, TopologyCodecError, decode_topology, encode_topology, start_health_monitor,
    start_peer_manager, start_replication_streams,
};
