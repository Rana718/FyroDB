use crate::storage::store::{Store, cgroup_memory_bytes, peak_rss_bytes, rss_bytes};
use crate::storage::value::{now_ms, tick_clock};

impl Store {
    pub fn cleanup_expired(&self) {
        tick_clock();
        let now = now_ms();
        let generation = self.ttl_generation();
        let mut live_ttls = 0usize;
        self.data.retain(|_, entry| {
            if entry.expires_ms == 0 {
                true
            } else if entry.expires_ms > now {
                live_ttls += 1;
                true
            } else {
                false
            }
        });
        self.finish_ttl_scan(generation, live_ttls);
    }

    pub fn cleanup_expired_shard(
        &self,
        shard: usize,
        start_slot: usize,
        max_slots: usize,
    ) -> (usize, usize, usize, usize) {
        if !self.has_ttl_keys() {
            return (0, start_slot, 0, 0);
        }
        tick_clock();
        let now = now_ms();
        let mut live_ttls = 0usize;
        let mut removed = 0usize;
        let (next_slot, capacity) = self
            .data
            .retain_shard_range(shard, start_slot, max_slots, |_, entry| {
                if entry.expires_ms != 0 && entry.expires_ms <= now {
                    self.sub_ttl();
                    removed += 1;
                    false
                } else {
                    if entry.expires_ms != 0 {
                        live_ttls += 1;
                    }
                    true
                }
            })
            .unwrap_or((start_slot, 0));
        (live_ttls, next_slot, capacity, removed)
    }

    pub fn info(&self) -> String {
        let total_keys = self.data.len();
        let connected = self.connected_clients();
        let allocated = rust_zmalloc::used_memory();
        let rss = rss_bytes();
        let peak_rss = peak_rss_bytes();
        let cgroup = cgroup_memory_bytes();
        let rss_human = format_bytes(rss);
        let frag_ratio = if allocated > 0 {
            rss as f64 / allocated as f64
        } else {
            0.0
        };

        let cluster_info = if self.cluster.enabled {
            let primaries = self
                .cluster
                .topology
                .nodes
                .iter()
                .filter(|node| node.role == crate::cluster::NodeRole::Primary)
                .count();
            let assigned: usize = self
                .cluster
                .topology
                .nodes
                .iter()
                .filter(|node| node.role == crate::cluster::NodeRole::Primary)
                .flat_map(|node| node.slots.iter())
                .map(|range| usize::from(range.end.value() - range.start.value()) + 1)
                .sum();
            let log_len = self.replication.as_ref().map_or(0, |log| log.len());
            let log_bytes = self.replication.as_ref().map_or(0, |log| log.retained_bytes());
            let log_byte_limit = self
                .replication
                .as_ref()
                .map_or(0, |log| log.retained_byte_limit());
            let next_offset = self.replication.as_ref().map_or(0, |log| log.next_offset());
            let appended = self
                .replication
                .as_ref()
                .map_or(0, |log| log.appended_count());
            format!(
                "# Cluster\r\ncluster_enabled:1\r\ncluster_state:{}\r\ncluster_my_id:{}\r\ncluster_current_epoch:{}\r\ncluster_known_nodes:{}\r\ncluster_size:{}\r\ncluster_slots_assigned:{}\r\ncluster_replication_log_len:{}\r\ncluster_replication_log_bytes:{}\r\ncluster_replication_log_byte_limit:{}\r\ncluster_replication_next_offset:{}\r\ncluster_replication_appended:{}\r\ncluster_peer_health:unknown\r\n\r\n",
                if self.cluster.topology.is_complete() {
                    "ok"
                } else {
                    "fail"
                },
                self.cluster.local_id,
                self.cluster.topology.epoch,
                self.cluster.topology.nodes.len(),
                primaries,
                assigned,
                log_len,
                log_bytes,
                log_byte_limit,
                next_offset,
                appended
            )
        } else {
            String::new()
        };

        format!(
            "# Server\r\n\
             fyrodb_version:{version}\r\n\
             os:{os}\r\n\
             arch:{arch}\r\n\
             \r\n\
             # Clients\r\n\
             connected_clients:{connected}\r\n\
             \r\n\
             # Memory\r\n\
             used_memory:{allocated}\r\n\
             used_memory_human:{allocated_human}\r\n\
             used_memory_rss:{rss}\r\n\
             used_memory_rss_human:{rss_human}\r\n\
             used_memory_peak:{peak_rss}\r\n\
             used_memory_peak_human:{peak_human}\r\n\
             used_memory_cgroup:{cgroup}\r\n\
             used_memory_cgroup_human:{cgroup_human}\r\n\
             mem_fragmentation_ratio:{frag_ratio:.2}\r\n\
             \r\n\
             # Stats\r\n\
             total_keys:{total_keys}\r\n\
             {cluster_info}",
            os = std::env::consts::OS,
            arch = std::env::consts::ARCH,
            version = env!("CARGO_PKG_VERSION"),
            peak_human = format_bytes(peak_rss),
            allocated = allocated,
            allocated_human = format_bytes(allocated),
            cgroup_human = format_bytes(cgroup),
            cluster_info = cluster_info,
        )
    }

    pub fn flush(&self) {
        self.data.clear();
        self.reset_ttl_count();
        // Give EBR multiple rounds to advance epochs and reclaim retired
        // entries before asking the allocator to return pages to the OS.
        for _ in 0..4 {
            customhash::force_collect();
            std::thread::yield_now();
        }
        customhash::force_collect_quiescent();
        super::store::purge_allocator();
    }

    pub fn dbsize(&self) -> usize {
        self.data.len()
    }

    pub fn type_of(&self, key: &str) -> &'static str {
        match self.data.get_ref(key) {
            Some(e) if !e.is_expired_precise() => e.value.type_name(),
            _ => "none",
        }
    }
}

fn format_bytes(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = KB * 1024;
    const GB: usize = MB * 1024;

    if bytes >= GB {
        format!("{:.2}G", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2}M", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2}K", bytes as f64 / KB as f64)
    } else {
        format!("{}B", bytes)
    }
}
