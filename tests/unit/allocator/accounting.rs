use super::*;

#[test]
#[cfg(all(
   target_os = "linux",
   not(any(feature = "jemalloc", feature = "system"))
))]
fn allocator_keeps_user_page_candidate_policy() {
   const CHILD: &str = "FYRODB_ALLOCATOR_POLICY_TEST_CHILD";
   if std::env::var_os(CHILD).is_some() {
      unsafe extern "C" {
         fn mi_version() -> i32;
         fn mi_option_get(option: i32) -> libc::c_long;
      }
      configure_allocator();
      assert_eq!(
         unsafe { mi_version() } / 10_000,
         3,
         "review allocator option ABI after an upgrade"
      );
      assert_eq!(
         unsafe { mi_option_get(37) },
         7,
         "configuration overwrote the page-search policy"
      );
      return;
   }
   let result = std::process::Command::new(std::env::current_exe().unwrap())
      .args([
         "--exact",
         "tests::allocator_keeps_user_page_candidate_policy",
         "--nocapture",
      ])
      .env(CHILD, "1")
      .env("MIMALLOC_PAGE_MAX_CANDIDATES", "7")
      .output()
      .unwrap();
   assert!(
      result.status.success(),
      "{}",
      String::from_utf8_lossy(&result.stdout)
   );
}

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
