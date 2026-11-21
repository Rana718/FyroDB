use io_uring::{IoUring, opcode, types};
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;

pub struct SendRing {
   ring: IoUring,
   inflight: usize,
}

fn requested(setting: Option<&str>) -> bool {
   setting.is_some_and(|s| {
      ["true", "1", "yes", "on"]
         .iter()
         .any(|v| s.eq_ignore_ascii_case(v))
   })
}

impl SendRing {
   pub fn from_env() -> Option<Self> {
      if !requested(std::env::var("FYRODB_IO_URING").ok().as_deref()) {
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
         .or_else(|_| IoUring::new(1024))
         .and_then(Self::check_send);

      match ring {
         Ok(ring) => Some(Self { ring, inflight: 0 }),
         Err(e) => {
            eprintln!("fyrodb: io_uring unavailable ({e}); using write(2)");
            None
         }
      }
   }

   fn check_send(mut ring: IoUring) -> io::Result<IoUring> {
      let (sender, _receiver) = UnixStream::pair()?;
      let probe = opcode::Send::new(types::Fd(sender.as_raw_fd()), b"x".as_ptr(), 1)
         .flags(libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL)
         .build();
      // The probe uses static storage and runs before any client buffer is submitted.
      unsafe { ring.submission().push(&probe) }.expect("new submission queue is empty");
      loop {
         match ring.submit_and_wait(1) {
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            result => {
               result?;
            }
         }
         let result = ring.completion().next().map(|cqe| cqe.result());
         match result {
            Some(1) => return Ok(ring),
            Some(n) if n < 0 => return Err(io::Error::from_raw_os_error(-n)),
            Some(_) => return Err(io::Error::other("incomplete io_uring test send")),
            None => continue,
         }
      }
   }

   #[inline(always)]
   /// # Safety
   /// The buffer and fd must remain valid and unchanged until complete returns.
   pub unsafe fn push(
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

   pub fn complete(&mut self, out: &mut Vec<(u64, i32)>) {
      while self.inflight != 0 {
         match self.ring.submit_and_wait(1) {
            Ok(_) => {}
            Err(e) if matches!(e.raw_os_error(), Some(libc::EINTR | libc::EBUSY)) => continue,
            Err(e) => {
               // Returning with borrowed buffers in flight would permit use-after-free.
               eprintln!("fyrodb: unrecoverable io_uring submission failure: {e}");
               std::process::abort();
            }
         }
         for cqe in self.ring.completion() {
            out.push((cqe.user_data(), cqe.result()));
            self.inflight -= 1;
         }
      }
   }
}

#[cfg(test)]
mod tests {
   use super::*;

   #[test]
   fn ring_setup_requires_an_explicit_opt_in() {
      for setting in [
         None,
         Some(""),
         Some("false"),
         Some("off"),
         Some("0"),
         Some("invalid"),
      ] {
         assert!(!requested(setting));
      }
      for setting in ["true", "TRUE", "1", "yes", "on"] {
         assert!(requested(Some(setting)));
      }
   }

   #[test]
   fn sends_complete_before_borrowed_buffers_are_reused() {
      use std::io::Read;
      use std::os::fd::AsRawFd;
      let ring = match IoUring::new(8).and_then(SendRing::check_send) {
         Ok(ring) => ring,
         Err(error) if matches!(error.raw_os_error(), Some(libc::EPERM | libc::ENOSYS)) => {
            eprintln!("io_uring runtime test unavailable: {error}");
            return;
         }
         Err(error) => panic!("io_uring setup: {error}"),
      };
      let mut ring = SendRing { ring, inflight: 0 };
      let mut pairs = Vec::new();
      let mut buffers = Vec::new();
      let mut out = Vec::new();
      for i in 0..4 {
         let pair = std::os::unix::net::UnixStream::pair().unwrap();
         pair.0.set_nonblocking(true).unwrap();
         pair
            .1
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
         pairs.push(pair);
         buffers.push(vec![b'a' + i; 32]);
      }
      for (i, ((writer, _), buffer)) in pairs.iter().zip(&buffers).enumerate() {
         // All sockets and buffers remain alive and unchanged through complete.
         unsafe {
            ring.push(
               writer.as_raw_fd(),
               buffer.as_ptr(),
               buffer.len(),
               i as u64,
               &mut out,
            )
         };
      }
      ring.complete(&mut out);
      assert_eq!(ring.inflight, 0);
      out.sort_unstable();
      assert_eq!(out, vec![(0, 32), (1, 32), (2, 32), (3, 32)]);
      for (i, ((_, reader), buffer)) in pairs.iter_mut().zip(&mut buffers).enumerate() {
         buffer.fill(b'z');
         let mut received = [0; 32];
         reader.read_exact(&mut received).unwrap();
         assert_eq!(received, [b'a' + i as u8; 32]);
      }
   }
}
