//! TTL-key accounting: CPU-striped add/remove counters whose monotonic
//! add count doubles as the expiry-scan generation.

use super::*;


/// Counters striped by CPU; `adds` is monotonic and doubles as the
/// scan generation.
pub(super) struct TtlCounters {
    pub(super) adds: Box<[CachePadded<AtomicU64>]>,
    pub(super) removes: Box<[CachePadded<AtomicU64>]>,
}

impl TtlCounters {
    pub(super) fn new() -> Self {
        let stripes = ttl_stripes();
        Self {
            adds: (0..stripes)
                .map(|_| CachePadded::new(AtomicU64::new(0)))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            removes: (0..stripes)
                .map(|_| CachePadded::new(AtomicU64::new(0)))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    #[inline(always)]
    pub(super) fn stripe(&self) -> usize {
        #[cfg(target_os = "linux")]
        {
            let cpu = unsafe { libc::sched_getcpu() };
            if cpu < 0 {
                0
            } else {
                cpu as usize % self.adds.len()
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            0
        }
    }

    #[inline]
    pub(super) fn add(&self) {
        let index = self.stripe();
        unsafe { self.adds.get_unchecked(index) }.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(super) fn remove(&self) {
        let index = self.stripe();
        unsafe { self.removes.get_unchecked(index) }.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn total_adds(&self) -> u64 {
        self.adds
            .iter()
            .map(|stripe| stripe.load(Ordering::Acquire))
            .fold(0u64, u64::wrapping_add)
    }

    pub(super) fn total_removes(&self) -> u64 {
        self.removes
            .iter()
            .map(|stripe| stripe.load(Ordering::Acquire))
            .fold(0u64, u64::wrapping_add)
    }

    /// Live TTL keys, saturating at zero: `removes` can momentarily lead
    /// `adds` across stripes, and an over-count is repaired by the next scan.
    pub(super) fn live(&self) -> usize {
        self.total_adds().saturating_sub(self.total_removes()) as usize
    }

    /// Force the live count to `live_ttls` by moving the removes counter.
    pub(super) fn set_live(&self, live_ttls: usize) {
        let adds = self.total_adds();
        let target_removes = adds.saturating_sub(live_ttls as u64);
        let current = self.total_removes();
        if target_removes > current {
            self.removes[0].fetch_add(target_removes - current, Ordering::Release);
        } else if current > target_removes {
            self.removes[0].fetch_sub(current - target_removes, Ordering::Release);
        }
    }
}

pub(super) fn ttl_stripes() -> usize {
    num_cpus::get().next_power_of_two().clamp(1, 64)
}

impl Store {
    /// One uncontended increment on this CPU's stripe.
    #[inline]
    pub fn add_ttl(&self) {
        self.ttl.add();
    }

    #[inline]
    pub fn sub_ttl(&self) {
        self.ttl.remove();
    }

    #[inline]
    pub fn has_ttl_keys(&self) -> bool {
        self.ttl.live() > 0
    }

    pub fn reset_ttl_count(&self) {
        self.ttl.set_live(0);
    }

/// Doubles as the scan generation: changes only when a TTL is added.
    #[inline]
    pub fn ttl_generation(&self) -> u64 {
        self.ttl.total_adds()
    }

    #[inline]
    pub fn finish_ttl_scan(&self, generation: u64, live_ttls: usize) {
        // A TTL created while the scan ran makes its tally stale; leave the
        // counter alone and let the next pass repair it.
        if self.ttl.total_adds() != generation {
            return;
        }
        self.ttl.set_live(live_ttls);
    }

    #[cfg(test)]
    pub(super) fn ttl_live(&self) -> usize {
        self.ttl.live()
    }
}
