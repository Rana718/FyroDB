pub const OK: &[u8] = b"+OK\r\n";
pub const NIL: &[u8] = b"$-1\r\n";
pub const ZERO: &[u8] = b":0\r\n";
pub const ONE: &[u8] = b":1\r\n";
pub const PONG: &[u8] = b"+PONG\r\n";

static INT_CACHE: [&[u8]; 10] = [
   b":0\r\n", b":1\r\n", b":2\r\n", b":3\r\n", b":4\r\n", b":5\r\n", b":6\r\n", b":7\r\n",
   b":8\r\n", b":9\r\n",
];

#[inline]
pub fn write_ok(out: &mut Vec<u8>) {
   out.extend_from_slice(OK);
}

#[inline]
pub fn write_pong(out: &mut Vec<u8>) {
   out.extend_from_slice(PONG);
}

#[inline]
pub fn write_nil(out: &mut Vec<u8>) {
   out.extend_from_slice(NIL);
}

#[inline]
pub fn write_bulk(out: &mut Vec<u8>, s: &str) {
   write_bulk_bytes(out, s.as_bytes());
}

#[cold]
#[inline(never)]
fn reserve_resp(out: &mut Vec<u8>, total: usize) {
   out.reserve(total);
}

#[inline(always)]
pub fn write_bulk_bytes(out: &mut Vec<u8>, s: &[u8]) {
   let len = s.len();
   if len < 10 {
      let old_len = out.len();
      let total = 4 + len + 2;
      if out.capacity() - old_len < total {
         reserve_resp(out, total);
      }
      unsafe {
         let p = out.as_mut_ptr().add(old_len);
         p.cast::<u32>().write_unaligned(u32::from_ne_bytes([
            b'$',
            b'0' + len as u8,
            b'\r',
            b'\n',
         ]));
         std::ptr::copy_nonoverlapping(s.as_ptr(), p.add(4), len);
         p.add(4 + len)
            .cast::<u16>()
            .write_unaligned(u16::from_ne_bytes(*b"\r\n"));
         out.set_len(old_len + total);
      }
      return;
   }
   if len < 100 {
      let old_len = out.len();
      let total = 5 + len + 2;
      if out.capacity() - old_len < total {
         reserve_resp(out, total);
      }
      unsafe {
         let p = out.as_mut_ptr().add(old_len);
         *p = b'$';
         *p.add(1) = b'0' + (len / 10) as u8;
         *p.add(2) = b'0' + (len % 10) as u8;
         *p.add(3) = b'\r';
         *p.add(4) = b'\n';
         std::ptr::copy_nonoverlapping(s.as_ptr(), p.add(5), len);
         *p.add(5 + len) = b'\r';
         *p.add(6 + len) = b'\n';
         out.set_len(old_len + total);
      }
      return;
   }
   out.reserve(1 + 10 + 2 + len + 2);
   out.push(b'$');
   write_usize(out, len);
   out.extend_from_slice(b"\r\n");
   out.extend_from_slice(s);
   out.extend_from_slice(b"\r\n");
}

#[inline(always)]
pub fn write_integer(out: &mut Vec<u8>, n: i64) {
   if (0..10).contains(&n) {
      out.extend_from_slice(INT_CACHE[n as usize]);
      return;
   }
   let mut buf = [0u8; 32];
   buf[0] = b':';
   let mut pos = 1;
   let mut val = if n < 0 {
      buf[pos] = b'-';
      pos += 1;
      (-n) as u64
   } else {
      n as u64
   };
   let mut digits = [0u8; 20];
   let mut d_pos = 20;
   loop {
      d_pos -= 1;
      digits[d_pos] = b'0' + (val % 10) as u8;
      val /= 10;
      if val == 0 {
         break;
      }
   }
   let d_len = 20 - d_pos;
   buf[pos..pos + d_len].copy_from_slice(&digits[d_pos..]);
   pos += d_len;
   buf[pos] = b'\r';
   buf[pos + 1] = b'\n';
   pos += 2;
   out.extend_from_slice(&buf[..pos]);
}

#[inline]
pub fn write_boolean(out: &mut Vec<u8>, b: bool) {
   out.extend_from_slice(if b { ONE } else { ZERO });
}

#[inline]
pub fn write_opt_bulk(out: &mut Vec<u8>, v: Option<String>) {
   match v {
      Some(s) => write_bulk(out, &s),
      None => write_nil(out),
   }
}

#[inline]
pub fn write_opt_bulk_ref(out: &mut Vec<u8>, v: Option<&str>) {
   match v {
      Some(s) => write_bulk(out, s),
      None => write_nil(out),
   }
}

#[inline]
pub fn write_array_header(out: &mut Vec<u8>, len: usize) {
   out.push(b'*');
   write_usize(out, len);
   out.extend_from_slice(b"\r\n");
}

#[inline]
pub fn write_array(out: &mut Vec<u8>, items: &[String]) {
   write_array_header(out, items.len());
   for item in items {
      write_bulk(out, item);
   }
}

#[inline]
pub fn write_opt_array(out: &mut Vec<u8>, items: &[Option<String>]) {
   write_array_header(out, items.len());
   for item in items {
      match item {
         Some(s) => write_bulk(out, s),
         None => write_nil(out),
      }
   }
}

#[inline]
pub fn write_err(out: &mut Vec<u8>, msg: &str) {
   out.extend_from_slice(b"-ERR ");
   out.extend_from_slice(msg.as_bytes());
   out.extend_from_slice(b"\r\n");
}

#[inline]
pub fn write_store_err(out: &mut Vec<u8>, err: &str) {
   if err == "WRONGTYPE" {
      write_wrong_type(out);
      return;
   }
   let code = err.split(' ').next().unwrap_or("");
   if code.len() >= 3 && err.len() > code.len() && code.bytes().all(|b| b.is_ascii_uppercase()) {
      out.push(b'-');
      out.extend_from_slice(err.as_bytes());
      out.extend_from_slice(b"\r\n");
   } else {
      write_err(out, err);
   }
}

#[inline]
pub fn write_wrong_type(out: &mut Vec<u8>) {
   out.extend_from_slice(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
}

#[inline]
pub fn write_wrong_args(out: &mut Vec<u8>, cmd: &str) {
   out.extend_from_slice(b"-ERR wrong number of arguments for '");
   out.extend_from_slice(cmd.as_bytes());
   out.extend_from_slice(b"' command\r\n");
}

#[inline]
pub fn write_simple(out: &mut Vec<u8>, s: &str) {
   out.push(b'+');
   out.extend_from_slice(s.as_bytes());
   out.extend_from_slice(b"\r\n");
}

#[inline(always)]
pub fn write_usize(out: &mut Vec<u8>, n: usize) {
   if n < 10 {
      out.push(b'0' + n as u8);
      return;
   }
   let mut buf = [0u8; 20];
   let mut pos = 20;
   let mut v = n;
   while v > 0 {
      pos -= 1;
      buf[pos] = b'0' + (v % 10) as u8;
      v /= 10;
   }
   out.extend_from_slice(&buf[pos..]);
}

#[inline(always)]
pub fn write_i64(out: &mut Vec<u8>, n: i64) {
   if n >= 0 {
      write_usize(out, n as usize);
      return;
   }
   out.push(b'-');
   let mut buf = [0u8; 20];
   let mut pos = 20;
   let mut v = n;
   loop {
      pos -= 1;
      buf[pos] = b'0' + (-(v % 10)) as u8;
      v /= 10;
      if v == 0 {
         break;
      }
   }
   out.extend_from_slice(&buf[pos..]);
}

#[inline]
pub fn bulk(s: &str) -> String {
   format!("${}\r\n{}\r\n", s.len(), s)
}
