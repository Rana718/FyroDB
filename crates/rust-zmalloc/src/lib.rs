use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(not(any(feature = "jemalloc", feature = "system")))]
const BACKEND: mimalloc::MiMalloc = mimalloc::MiMalloc;
#[cfg(feature = "jemalloc")]
const BACKEND: jemallocator::Jemalloc = jemallocator::Jemalloc;
#[cfg(feature = "system")]
const BACKEND: std::alloc::System = std::alloc::System;

#[inline(always)]
fn backend_alloc(layout: Layout) -> *mut u8 {
   unsafe { BACKEND.alloc(layout) }
}

#[inline(always)]
fn backend_dealloc(ptr: *mut u8, layout: Layout) {
   unsafe { BACKEND.dealloc(ptr, layout) }
}

#[inline(always)]
fn backend_alloc_zeroed(layout: Layout) -> *mut u8 {
   unsafe { BACKEND.alloc_zeroed(layout) }
}

#[inline(always)]
fn backend_realloc(ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
   unsafe { BACKEND.realloc(ptr, layout, new_size) }
}

const STRIPES: usize = 64;

#[repr(align(128))]
struct Stripe(std::sync::atomic::AtomicI64);

static ALLOCATED: [Stripe; STRIPES] =
   [const { Stripe(std::sync::atomic::AtomicI64::new(0)) }; STRIPES];

thread_local! {
    static DELTA: std::cell::Cell<i64> = const { std::cell::Cell::new(0) };
    static STRIPE: std::cell::Cell<usize> = const { std::cell::Cell::new(usize::MAX) };
}

#[cold]
#[inline(never)]
fn record_slow(d: i64) {
   STRIPE.with(|s_cell| {
      let mut stripe = s_cell.get();
      if stripe == usize::MAX {
         #[cfg(target_os = "linux")]
         let idx = unsafe { libc::sched_getcpu() }.max(0) as usize % STRIPES;
         #[cfg(not(target_os = "linux"))]
         let idx = 0;
         stripe = idx;
         s_cell.set(stripe);
      }
      ALLOCATED[stripe].0.fetch_add(d, Ordering::Relaxed);
   });
}

#[inline(always)]
fn record(delta: i64) {
   DELTA.with(|cell| {
      let d = cell.get() + delta;
      if d.unsigned_abs() >= 65536 {
         record_slow(d);
         cell.set(0);
      } else {
         cell.set(d);
      }
   });
}

pub struct Zmalloc;

unsafe impl GlobalAlloc for Zmalloc {
   #[inline]
   unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
      let ptr = backend_alloc(layout);
      if !ptr.is_null() {
         record(layout.size() as i64);
      }
      ptr
   }
   #[inline]
   unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
      record(-(layout.size() as i64));
      backend_dealloc(ptr, layout);
   }
   #[inline]
   unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
      let ptr = backend_alloc_zeroed(layout);
      if !ptr.is_null() {
         record(layout.size() as i64);
      }
      ptr
   }
   #[inline]
   unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
      let new_ptr = backend_realloc(ptr, layout, new_size);
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

/// Reads /proc/self/statm; maintenance/INFO cadences only.
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

#[cfg(all(not(any(feature = "jemalloc", feature = "system")), target_os = "linux"))]
#[used]
#[unsafe(link_section = ".init_array")]
static INIT_ALLOC: extern "C" fn() = {
   extern "C" fn init() {
      configure_allocator();
   }
   init
};

#[cfg(not(any(feature = "jemalloc", feature = "system")))]
pub fn configure_allocator() {
   static INIT: std::sync::Once = std::sync::Once::new();
   INIT.call_once(|| {
      unsafe extern "C" {
         fn mi_option_set(option: i32, value: i64);
      }
      unsafe {
         // mi_option_purge_delay = 15: 0 (immediate purge of unused pages to OS)
         mi_option_set(15, 0);
         // mi_option_arena_reserve = 23: 0 (don't pre-reserve 1GB arena upfront)
         mi_option_set(23, 0);
         // mi_option_arena_eager_commit = 4: 0 (don't eagerly commit arenas)
         mi_option_set(4, 0);
         // mi_option_eager_commit = 3: 0 (don't eagerly commit segments)
         mi_option_set(3, 0);
      }
   });
}

#[cfg(any(feature = "jemalloc", feature = "system"))]
pub fn configure_allocator() {}

/// Only mimalloc exposes forced collection; others decay on their own.
pub fn purge() {
   #[cfg(not(any(feature = "jemalloc", feature = "system")))]
   {
      unsafe extern "C" {
         fn mi_collect(force: bool);
      }
      unsafe { mi_collect(true) };
   }
}

/// >1.0 means fragmentation; Redis calls this mem_fragmentation_ratio.
pub fn fragmentation_ratio() -> f64 {
   let allocated = used_memory();
   if allocated == 0 {
      return 0.0;
   }
   rss_bytes_inner() as f64 / allocated as f64
}

/// # Safety
/// Caller must handle null return (allocation failure) and free with matching layout.
#[inline]
pub unsafe fn alloc_raw(layout: Layout) -> *mut u8 {
   let ptr = backend_alloc(layout);
   if !ptr.is_null() {
      record(layout.size() as i64);
   }
   ptr
}

/// # Safety
/// Caller must handle null return and free with matching layout.
#[inline]
pub unsafe fn alloc_raw_zeroed(layout: Layout) -> *mut u8 {
   let ptr = backend_alloc_zeroed(layout);
   if !ptr.is_null() {
      record(layout.size() as i64);
   }
   ptr
}

/// # Safety
/// `ptr` must have been allocated with the same layout via `alloc_raw` or `alloc_raw_zeroed`.
#[inline]
pub unsafe fn dealloc_raw(ptr: *mut u8, layout: Layout) {
   if !ptr.is_null() {
      record(-(layout.size() as i64));
      backend_dealloc(ptr, layout);
   }
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
#[path = "../../../tests/unit/allocator/accounting.rs"]
mod tests;
