use io_uring::{IoUring, opcode, types};
use std::os::fd::RawFd;

pub struct SendRing {
    ring: IoUring,
    inflight: usize,
}

impl SendRing {
    pub fn from_env() -> Option<Self> {
        let on = std::env::var("FYRODB_IO_URING").is_ok_and(|v| matches!(v.as_str(), "1" | "yes" | "true"));
        if !on {
            return None;
        }
        match IoUring::new(256) {
            Ok(ring) => Some(Self { ring, inflight: 0 }),
            Err(e) => {
                eprintln!("fyrodb: io_uring unavailable ({e}); using write(2)");
                None
            }
        }
    }

    pub fn push(&mut self, fd: RawFd, ptr: *const u8, len: usize, token: u64, out: &mut Vec<(u64, i32)>) {
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
        while self.inflight > 0 {
            if let Err(e) = self.ring.submit_and_wait(self.inflight) {
                if e.raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                eprintln!("fyrodb: io_uring submit failed: {e}");
                return;
            }
            for cqe in self.ring.completion() {
                out.push((cqe.user_data(), cqe.result()));
                self.inflight -= 1;
            }
        }
    }
}
