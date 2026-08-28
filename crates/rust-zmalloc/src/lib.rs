//! Memory control layer using mimalloc as the backing allocator.
//!
//! Provides a `GlobalAlloc` implementation with lightweight memory tracking,
//! RSS reporting, purge support, and raw allocation helpers for EBR-managed
//! data structures.

use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicUsize, Ordering};

static MIMALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Number of independent counters used to track live allocated bytes.
///
/// A single global counter would serialize every allocation on one cache line.
/// Striping by CPU keeps the update uncontended in the common case; a block
/// freed on a different CPU than it was allocated on simply moves the debt
/// between stripes, and the *sum* stays exact.
const STRIPES: usize = 64;

#[repr(align(128))]
struct Stripe(std::sync::atomic::AtomicI64);

static ALLOCATED: [Stripe; STRIPES] = {
    #[allow(clippy::declare_interior_mutable_const)]
    const ZERO: Stripe = Stripe(std::sync::atomic::AtomicI64::new(0));
    [ZERO; STRIPES]
};

/// Pick a stripe without allocating.
///
/// This runs inside `GlobalAlloc`, so it must not touch anything that could
/// allocate — notably `thread_local!`, whose lazy initialization would recurse
/// back into the allocator. `sched_getcpu` is a vDSO call with no such risk.
#[inline(always)]
fn stripe() -> &'static std::sync::atomic::AtomicI64 {
    #[cfg(target_os = "linux")]
    let index = {
        let cpu = unsafe { libc::sched_getcpu() };
        if cpu < 0 { 0 } else { cpu as usize % STRIPES }
    };
    #[cfg(not(target_os = "linux"))]
    let index = 0usize;
    // SAFETY: index is masked into range above.
    &unsafe { ALLOCATED.get_unchecked(index) }.0
}

#[inline(always)]
fn record(delta: i64) {
    if delta != 0 {
        stripe().fetch_add(delta, Ordering::Relaxed);
    }
}

pub struct Zmalloc;

unsafe impl GlobalAlloc for Zmalloc {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { MIMALLOC.alloc(layout) };
        if !ptr.is_null() {
            record(layout.size() as i64);
        }
        ptr
    }
    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        record(-(layout.size() as i64));
        unsafe { MIMALLOC.dealloc(ptr, layout) }
    }
    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { MIMALLOC.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record(layout.size() as i64);
        }
        ptr
    }
    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { MIMALLOC.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            record(new_size as i64 - layout.size() as i64);
        }
        new_ptr
    }
}

/// Live bytes requested from the allocator, the analogue of Redis's
/// `zmalloc_used_memory`. Exact to the granularity of the striped counters.
#[inline]
pub fn used_memory() -> usize {
    let mut total: i64 = 0;
    for stripe in ALLOCATED.iter() {
        total = total.saturating_add(stripe.0.load(Ordering::Relaxed));
    }
    total.max(0) as usize
}

/// Resident bytes the process holds from the OS.
///
/// Reads `/proc/self/statm`, which is cheap enough for the maintenance-thread
/// and `INFO` cadences that use it. Not for hot paths.
pub fn resident_memory() -> usize {
    rss_bytes_inner()
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Stats {
    pub allocated: usize,
    pub active: usize,
    pub resident: usize,
    pub retained: usize,
    pub muzzy: usize,
}

pub fn stats() -> Stats {
    let allocated = used_memory();
    let rss = rss_bytes_inner();
    Stats {
        allocated,
        active: allocated,
        resident: rss,
        retained: rss.saturating_sub(allocated),
        muzzy: 0,
    }
}

/// Force mimalloc to collect and return unused pages to the OS.
pub fn purge() {
    unsafe extern "C" {
        fn mi_collect(force: bool);
    }
    unsafe { mi_collect(true) };
}

#[inline]
pub fn refresh_epoch() {}

/// Resident bytes per byte the application actually asked for. Above 1.0 means
/// allocator or page-level fragmentation; Redis reports the same quantity as
/// `mem_fragmentation_ratio`.
pub fn fragmentation_ratio() -> f64 {
    let allocated = used_memory();
    if allocated == 0 {
        return 0.0;
    }
    rss_bytes_inner() as f64 / allocated as f64
}

/// Raw allocation helper for EBR-managed objects.
#[inline]
pub unsafe fn alloc_raw(layout: Layout) -> *mut u8 {
    let ptr = unsafe { MIMALLOC.alloc(layout) };
    if !ptr.is_null() {
        record(layout.size() as i64);
    }
    ptr
}

#[inline]
pub unsafe fn dealloc_raw(ptr: *mut u8, layout: Layout) {
    if !ptr.is_null() {
        record(-(layout.size() as i64));
        unsafe { MIMALLOC.dealloc(ptr, layout) }
    }
}

#[inline]
pub unsafe fn alloc_raw_no_tcache(layout: Layout) -> *mut u8 {
    unsafe { alloc_raw(layout) }
}

#[inline]
pub unsafe fn dealloc_raw_no_tcache(ptr: *mut u8, layout: Layout) {
    unsafe { dealloc_raw(ptr, layout) }
}

fn rss_bytes_inner() -> usize {
    #[cfg(target_os = "linux")]
    {
        // `/proc/self/statm` is a single short line; `/proc/self/status` is
        // ~60 lines the kernel formats on every read.
        let Ok(statm) = std::fs::read_to_string("/proc/self/statm") else {
            return 0;
        };
        let Some(resident_pages) = statm
            .split_ascii_whitespace()
            .nth(1)
            .and_then(|field| field.parse::<usize>().ok())
        else {
            return 0;
        };
        resident_pages.saturating_mul(page_size())
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

#[cfg(target_os = "linux")]
fn page_size() -> usize {
    static CACHED: AtomicUsize = AtomicUsize::new(0);
    match CACHED.load(Ordering::Relaxed) {
        0 => {
            let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
            let size = if size > 0 { size as usize } else { 4096 };
            CACHED.store(size, Ordering::Relaxed);
            size
        }
        cached => cached,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_alloc_preserves_alignment_and_contents() {
        for &(size, align) in &[(1, 1), (17, 8), (129, 64), (4096, 4096)] {
            let layout = Layout::from_size_align(size, align).unwrap();
            let ptr = unsafe { alloc_raw(layout) };
            assert!(!ptr.is_null());
            assert_eq!((ptr as usize) % align, 0);
            unsafe {
                std::ptr::write_bytes(ptr, 0xA5, size);
                assert_eq!(*ptr, 0xA5);
                assert_eq!(*ptr.add(size - 1), 0xA5);
                dealloc_raw(ptr, layout);
            }
        }
    }

    /// `used_memory` used to report RSS, which made it useless for the
    /// fragmentation ratio (rss/rss == 1) and for the purge/defrag triggers.
    #[test]
    fn used_memory_tracks_live_requested_bytes() {
        const SIZE: usize = 4 * 1024 * 1024;
        let layout = Layout::from_size_align(SIZE, 64).unwrap();

        let before = used_memory();
        let ptr = unsafe { alloc_raw(layout) };
        assert!(!ptr.is_null());
        let during = used_memory();
        unsafe { dealloc_raw(ptr, layout) };
        let after = used_memory();

        assert!(
            during >= before + SIZE,
            "expected used_memory to grow by at least {SIZE}, went {before} -> {during}"
        );
        assert!(
            after + SIZE / 2 < during,
            "expected used_memory to drop after free, {during} -> {after}"
        );
    }

    #[test]
    fn rss_reading_is_plausible() {
        let rss = rss_bytes_inner();
        assert!(rss > 0, "RSS reading should be non-zero on Linux");
        assert!(rss < 1 << 40);
    }
}
