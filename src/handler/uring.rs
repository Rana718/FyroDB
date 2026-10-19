use io_uring::{IoUring, opcode, types};
use std::os::fd::RawFd;

pub struct SendRing {
   ring: IoUring,
   inflight: usize,
}

impl SendRing {
   pub fn from_env() -> Option<Self> {
      let disabled = std::env::var("FYRODB_IO_URING")
         .is_ok_and(|v| matches!(v.as_str(), "0" | "no" | "false" | "off"));
      if disabled {
         return None;
      }

      // Try highest performance configuration: single issuer + defer taskrun + cqsize 4096.
      let ring = IoUring::<io_uring::squeue::Entry, io_uring::cqueue::Entry>::builder()
         .setup_single_issuer()
         .setup_defer_taskrun()
         .setup_cqsize(4096)
         .build(2048)
         .or_else(|_| {
            IoUring::<io_uring::squeue::Entry, io_uring::cqueue::Entry>::builder()
               .setup_single_issuer()
               .setup_coop_taskrun()
               .setup_taskrun_flag()
               .setup_cqsize(2048)
               .build(1024)
         })
         .or_else(|_| IoUring::new(1024));

      match ring {
         Ok(ring) => Some(Self { ring, inflight: 0 }),
         Err(e) => {
            eprintln!("fyrodb: io_uring unavailable ({e}); using write(2)");
            None
         }
      }
   }

   #[inline(always)]
   pub fn push(
      &mut self,
      fd: RawFd,
      ptr: *const u8,
      len: usize,
      token: u64,
      out: &mut Vec<(u64, i32)>,
   ) {
      let len = len.min(u32::MAX as usize) as u32;
      let e = opcode::Send::new(types::Fd(fd), ptr, len)
         .flags(libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL)
         .build()
         .user_data(token);
      loop {
         if unsafe { self.ring.submission().push(&e) }.is_ok() {
            self.inflight += 1;
            return;
         }
         self.complete(out);
      }
   }

   #[inline(always)]
   pub fn submit_and_reap(&mut self, out: &mut Vec<(u64, i32)>) {
      if self.inflight == 0 {
         return;
      }
      let _ = self.ring.submit_and_wait(0);
      for cqe in self.ring.completion() {
         out.push((cqe.user_data(), cqe.result()));
         self.inflight = self.inflight.saturating_sub(1);
      }
   }

   pub fn complete(&mut self, out: &mut Vec<(u64, i32)>) {
      if self.inflight == 0 {
         return;
      }
      loop {
         match self.ring.submit() {
            Ok(_) => break,
            Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
            Err(e) => {
               eprintln!("fyrodb: io_uring submit failed: {e}");
               self.inflight = 0;
               return;
            }
         }
      }
      for cqe in self.ring.completion() {
         out.push((cqe.user_data(), cqe.result()));
         self.inflight = self.inflight.saturating_sub(1);
      }
   }
}

#[cfg(test)]
mod tests {
   use super::*;

   #[test]
   fn test_builder() {
      let ring = IoUring::<io_uring::squeue::Entry, io_uring::cqueue::Entry>::builder()
         .setup_single_issuer()
         .setup_defer_taskrun()
         .setup_cqsize(4096)
         .build(2048);
      println!("builder with single_issuer + defer_taskrun + cqsize 4096: {:?}", ring.as_ref().map(|_| "ok"));
      assert!(ring.is_ok());
   }
}
