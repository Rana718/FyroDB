use std::time::{SystemTime, UNIX_EPOCH};

pub fn mix64(mut value: u64) -> u64 {
   value ^= value >> 30;
   value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
   value ^= value >> 27;
   value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
   value ^ (value >> 31)
}

/// Wall-clock milliseconds for cache expiry.
///
/// This used to be a per-thread value refreshed every 1,024 calls, which is
/// a call budget rather than a time bound: on a quiet proxy connection (one
/// thread per client) "now" could lag by minutes, so expired results kept
/// being served past their TTL and new entries were stamped with deadlines
/// already in the past. The coarse realtime clock is a vDSO read costing a
/// few nanoseconds with 1-4 ms resolution, which is plenty for TTLs.
#[inline(always)]
pub fn now_ms() -> u64 {
   #[cfg(target_os = "linux")]
   {
      let mut ts = libc::timespec {
         tv_sec: 0,
         tv_nsec: 0,
      };
      // SAFETY: `ts` is a valid out-pointer; CLOCK_REALTIME_COARSE always exists on Linux.
      if unsafe { libc::clock_gettime(libc::CLOCK_REALTIME_COARSE, &mut ts) } == 0 {
         return ts.tv_sec as u64 * 1_000 + ts.tv_nsec as u64 / 1_000_000;
      }
   }
   SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .map(|d| d.as_millis() as u64)
      .unwrap_or(0)
}

#[inline(always)]
pub fn coarse_now_ms() -> u64 {
   now_ms()
}

pub(super) fn expiry_ms(ttl_ms: u64) -> u64 {
   if ttl_ms == 0 { 0 } else { now_ms() + ttl_ms }
}

pub(super) fn jittered_ttl(ttl_ms: u64, jitter_pct: u64, seed: u64) -> u64 {
   if ttl_ms == 0 || jitter_pct == 0 {
      return ttl_ms;
   }
   let span = (ttl_ms / 100)
      .saturating_mul(jitter_pct.clamp(0, 50))
      .max(1);
   ttl_ms + (mix64(seed) % (span + 1))
}

#[cfg(test)]
mod tests {
   use super::*;

   #[test]
   fn the_clock_advances_on_a_thread_that_rarely_reads_it() {
      let before = now_ms();
      std::thread::sleep(std::time::Duration::from_millis(1_100));
      let after = now_ms();
      assert!(after >= before + 1_000, "clock stuck: {before} -> {after}");
      let wall = SystemTime::now()
         .duration_since(UNIX_EPOCH)
         .unwrap()
         .as_millis() as u64;
      assert!(
         after.abs_diff(wall) < 50,
         "clock {after} far from wall time {wall}"
      );
   }
}
