pub fn rss_bytes() -> usize {
   rust_zmalloc::resident_memory()
}

pub fn data_memory_bytes() -> usize {
   rust_zmalloc::used_memory()
}

pub fn allocated_bytes() -> usize {
   rust_zmalloc::used_memory()
}

pub fn peak_rss_bytes() -> usize {
   proc_status_kb("VmHWM:").saturating_mul(1024)
}

pub fn purge_allocator() {
   rust_zmalloc::purge();
}

/// used_memory is live requested bytes, rss what the OS mapped; the
/// gap is real fragmentation.
pub fn purge_allocator_if_fragmented() {
   let used = rust_zmalloc::used_memory();
   let rss = rust_zmalloc::resident_memory();
   // If RSS is more than 20% above used memory, trigger a purge
   if rss > used.saturating_add(used / 5) && rss.saturating_sub(used) >= 10 * 1024 * 1024 {
      rust_zmalloc::purge();
   }
}

pub fn cgroup_memory_bytes() -> usize {
   [
      "/sys/fs/cgroup/memory.current",
      "/sys/fs/cgroup/memory/memory.usage_in_bytes",
   ]
   .iter()
   .find_map(|path| std::fs::read_to_string(path).ok()?.trim().parse().ok())
   .unwrap_or(0)
}

fn proc_status_kb(name: &str) -> usize {
   std::fs::read_to_string("/proc/self/status")
      .ok()
      .and_then(|status| {
         status.lines().find_map(|line| {
            let value = line.strip_prefix(name)?;
            value.split_whitespace().next()?.parse().ok()
         })
      })
      .unwrap_or(0)
}
