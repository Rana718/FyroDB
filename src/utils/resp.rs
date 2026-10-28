#[inline(always)]
pub fn fast_parse_f64_bytes(b: &[u8]) -> Option<f64> {
   if b.is_empty() {
      return None;
   }
   let mut i = 0;
   let neg = if b[0] == b'-' {
      i += 1;
      true
   } else if b[0] == b'+' {
      i += 1;
      false
   } else {
      false
   };
   if i == b.len() {
      return None;
   }
   let mut int_part: u64 = 0;
   let mut has_int = false;
   while i < b.len() && b[i].is_ascii_digit() {
      has_int = true;
      int_part = int_part.wrapping_mul(10).wrapping_add((b[i] - b'0') as u64);
      i += 1;
   }
   if i == b.len() && has_int {
      let f = int_part as f64;
      return Some(if neg { -f } else { f });
   }
   if i < b.len() && b[i] == b'.' {
      i += 1;
      let mut frac_part: u64 = 0;
      let mut frac_div: f64 = 1.0;
      let mut has_frac = false;
      while i < b.len() && b[i].is_ascii_digit() && frac_div < 1e15 {
         has_frac = true;
         frac_part = frac_part * 10 + (b[i] - b'0') as u64;
         frac_div *= 10.0;
         i += 1;
      }
      if i == b.len() && (has_int || has_frac) {
         let f = int_part as f64 + (frac_part as f64 / frac_div);
         return Some(if neg { -f } else { f });
      }
   }
   std::str::from_utf8(b)
      .ok()
      .and_then(|s| s.parse::<f64>().ok())
}

#[inline(always)]
pub fn fast_parse_f64(s: &str) -> Option<f64> {
   fast_parse_f64_bytes(s.as_bytes())
}

pub const OK: &[u8] = b"+OK\r\n";
pub const NIL: &[u8] = b"$-1\r\n";
pub const ZERO: &[u8] = b":0\r\n";
pub const ONE: &[u8] = b":1\r\n";
pub const PONG: &[u8] = b"+PONG\r\n";

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
   out.push(b'$');
   write_usize(out, s.len());
   out.extend_from_slice(b"\r\n");
   out.extend_from_slice(s);
   out.extend_from_slice(b"\r\n");
}

#[inline(always)]
pub fn write_integer(out: &mut Vec<u8>, n: i64) {
   out.push(b':');
   write_i64(out, n);
   out.extend_from_slice(b"\r\n");
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

use crate::utils::int_format::DIGITS100;

#[inline(always)]
pub fn write_usize(out: &mut Vec<u8>, n: usize) {
   if n < 100 {
      if n < 10 {
         out.push(b'0' + n as u8);
      } else {
         let rem = n * 2;
         out.extend_from_slice(&DIGITS100[rem..rem + 2]);
      }
      return;
   }
   let mut digits = [0u8; 20];
   let s = crate::utils::int_format::write_usize_to(&mut digits, n);
   let d_len = s.len();
   let old_len = out.len();
   if out.capacity() - old_len < d_len {
      reserve_resp(out, d_len);
   }
   unsafe {
      std::ptr::copy_nonoverlapping(
         s.as_ptr(),
         out.as_mut_ptr().add(old_len),
         d_len,
      );
      out.set_len(old_len + d_len);
   }
}

#[inline(always)]
pub fn write_i64(out: &mut Vec<u8>, n: i64) {
   if n >= 0 {
      write_usize(out, n as usize);
      return;
   }

   let mut digits = [0u8; 20];
   let s = crate::utils::int_format::write_i64_to(&mut digits, n);
   out.extend_from_slice(s.as_bytes());
}

