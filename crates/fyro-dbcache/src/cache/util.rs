use std::time::{SystemTime, UNIX_EPOCH};

pub fn mix64(mut value: u64) -> u64 {
   value ^= value >> 30;
   value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
   value ^= value >> 27;
   value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
   value ^ (value >> 31)
}

thread_local! {
   static NOW_CACHE: std::cell::Cell<(u64, u16)> = const { std::cell::Cell::new((0, 0)) };
}

#[inline(always)]
pub fn now_ms() -> u64 {
   NOW_CACHE.with(|c| {
      let (cached, count) = c.get();
      if count == 0 {
         let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
         c.set((now, 1023));
         now
      } else {
         c.set((cached, count - 1));
         cached
      }
   })
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
