use foldhash::HashSet;
use mio::net::TcpStream;
use std::io::{self, Read, Write};
use std::sync::Arc;

use crate::pubsub::{PubSub, SubSlot, WorkerNotifier};
use crate::storage::store::Store;
use crate::utils::parser::{ParseResult, RespParser};

use super::dispatch::dispatch;
use super::subscription::do_full_unsubscribe;

pub(crate) const SUB_WRITE_BATCH_BYTES: usize = 256 * 1024;
const RETAINED_WRITE_BUFFER: usize = 32 * 1024;

mod coalesce;
mod fast_path;
pub mod park;
mod prefetch;
mod sock;

use coalesce::*;
use fast_path::*;


pub enum ConnMode {
    Normal,
    Subscribed {
        slot: Arc<SubSlot>,
        channels: HashSet<String>,
        patterns: HashSet<String>,
    },
}

pub struct Conn<'a> {
    pub stream: TcpStream,
    pub parser: RespParser,
    pub store: &'a Arc<Store>,
    pub pubsub: &'a PubSub,
    pub write_offset: usize,
    pub mode: ConnMode,
    pub token: usize,
    pub notifier: &'a Arc<WorkerNotifier>,
    pub auth_required: Option<&'a str>,
    pub authenticated: bool,
    pub asking: bool,
    pub writable_registered: bool,
    pub worker: usize,
    /// Process-unique id (tokens are recycled; this is not).
    pub uid: u64,
    /// Set while the current command waits off-worker or on a key.
    pub park: Option<park::Parked>,
    hints: [prefetch::Hint; customhash::PREFETCH_BATCH],
    hint_n: u8,
    pf_backoff: u8,
    topology_cache: Option<(
        u64,
        Arc<crate::cluster::Topology>,
        Arc<crate::cluster::RoutingTable>,
    )>,
}

impl<'a> Conn<'a> {
    pub fn new(
        stream: TcpStream,
        store: &'a Arc<Store>,
        pubsub: &'a PubSub,
        token: usize,
        notifier: &'a Arc<WorkerNotifier>,
        auth: Option<&'a str>,
        worker: usize,
    ) -> Self {
        store.client_connected();
        let authenticated = auth.is_none();
        Self {
            stream,
            parser: RespParser::new(),
            store,
            pubsub,
            write_offset: 0,
            mode: ConnMode::Normal,
            token,
            notifier,
            auth_required: auth,
            authenticated,
            asking: false,
            writable_registered: false,
            worker,
            uid: park::next_uid(),
            park: None,
            hints: [prefetch::Hint::default(); customhash::PREFETCH_BATCH],
            hint_n: 0,
            pf_backoff: 0,
            topology_cache: None,
        }
    }

    /// Bring the cached topology up to date. One `Acquire` load in the steady
    /// state; only a version change pays for the lock and `Arc` clones.
    #[inline]
    fn refresh_topology(&mut self) {
        let state = self.store.cluster_state_ref();
        match self.topology_cache.as_ref().map(|(version, ..)| *version) {
            Some(version) => {
                if let Some(fresh) = state.topology_if_newer(version) {
                    self.topology_cache = Some(fresh);
                }
            }
            None => {
                self.topology_cache = state.topology_if_newer(0);
            }
        }
    }

    #[inline]
    fn cached_topology(&self) -> (&crate::cluster::Topology, &crate::cluster::RoutingTable) {
        let (_, topology, routing) = self
            .topology_cache
            .as_ref()
            .expect("refresh_topology must run first");
        (topology.as_ref(), routing.as_ref())
    }

}

impl Drop for Conn<'_> {
    fn drop(&mut self) {
        self.unblock();
        do_full_unsubscribe(self);
        self.store.client_disconnected();
    }
}

#[inline(always)]
pub(super) unsafe fn part_bytes<'a>(part: (*const u8, usize)) -> &'a [u8] {
    unsafe { std::slice::from_raw_parts(part.0, part.1) }
}

#[inline(always)]
pub(super) fn part_str<'a>(out: &mut Vec<u8>, part: (*const u8, usize)) -> Option<&'a str> {
    let bytes: &'a [u8] = unsafe { part_bytes(part) };
    if bytes.is_ascii() {
        return Some(unsafe { std::str::from_utf8_unchecked(bytes) });
    }
    match std::str::from_utf8(bytes) {
        Ok(s) => Some(s),
        Err(_) => {
            crate::utils::resp::write_err(out, "invalid UTF-8 in request");
            None
        }
    }
}
